package downloader

import (
	"context"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/samber/lo"

	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/plans"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type DownloadNodeInfo struct {
	Node         cdssdk.Node
	ObjectPinned bool
	Blocks       []stgmod.ObjectBlock
	Distance     float64
}

type DownloadContext struct {
	Distlock *distlock.Service
}
type DownloadObjectIterator struct {
	OnClosing func()

	downloader   *Downloader
	reqs         []downloadReqeust2
	currentIndex int
	inited       bool

	coorCli  *coormq.Client
	allNodes map[cdssdk.NodeID]cdssdk.Node
}

func NewDownloadObjectIterator(downloader *Downloader, downloadObjs []downloadReqeust2) *DownloadObjectIterator {
	return &DownloadObjectIterator{
		downloader: downloader,
		reqs:       downloadObjs,
	}
}

func (i *DownloadObjectIterator) MoveNext() (*Downloading, error) {
	if !i.inited {
		if err := i.init(); err != nil {
			return nil, err
		}

		i.inited = true
	}

	if i.currentIndex >= len(i.reqs) {
		return nil, iterator.ErrNoMoreItem
	}

	item, err := i.doMove()
	i.currentIndex++
	return item, err
}

func (i *DownloadObjectIterator) init() error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	i.coorCli = coorCli

	allNodeIDs := make(map[cdssdk.NodeID]bool)
	for _, obj := range i.reqs {
		if obj.Detail == nil {
			continue
		}

		for _, p := range obj.Detail.PinnedAt {
			allNodeIDs[p] = true
		}

		for _, b := range obj.Detail.Blocks {
			allNodeIDs[b.NodeID] = true
		}
	}

	getNodes, err := coorCli.GetNodes(coormq.NewGetNodes(lo.Keys(allNodeIDs)))
	if err != nil {
		return fmt.Errorf("getting nodes: %w", err)
	}

	i.allNodes = make(map[cdssdk.NodeID]cdssdk.Node)
	for _, n := range getNodes.Nodes {
		i.allNodes[n.NodeID] = n
	}

	return nil
}

func (iter *DownloadObjectIterator) doMove() (*Downloading, error) {
	req := iter.reqs[iter.currentIndex]
	if req.Detail == nil {
		return &Downloading{
			Object:  nil,
			File:    nil,
			Request: req.Raw,
		}, nil
	}

	switch red := req.Detail.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		reader, err := iter.downloadNoneOrRepObject(req)
		if err != nil {
			return nil, fmt.Errorf("downloading object: %w", err)
		}

		return &Downloading{
			Object:  &req.Detail.Object,
			File:    reader,
			Request: req.Raw,
		}, nil

	case *cdssdk.RepRedundancy:
		reader, err := iter.downloadNoneOrRepObject(req)
		if err != nil {
			return nil, fmt.Errorf("downloading rep object: %w", err)
		}

		return &Downloading{
			Object:  &req.Detail.Object,
			File:    reader,
			Request: req.Raw,
		}, nil

	case *cdssdk.ECRedundancy:
		reader, err := iter.downloadECObject(req, red)
		if err != nil {
			return nil, fmt.Errorf("downloading ec object: %w", err)
		}

		return &Downloading{
			Object:  &req.Detail.Object,
			File:    reader,
			Request: req.Raw,
		}, nil
	}

	return nil, fmt.Errorf("unsupported redundancy type: %v", reflect.TypeOf(req.Detail.Object.Redundancy))
}

func (i *DownloadObjectIterator) Close() {
	if i.OnClosing != nil {
		i.OnClosing()
	}
}

func (iter *DownloadObjectIterator) downloadNoneOrRepObject(obj downloadReqeust2) (io.ReadCloser, error) {
	allNodes, err := iter.sortDownloadNodes(obj)
	if err != nil {
		return nil, err
	}

	bsc, blocks := iter.getMinReadingBlockSolution(allNodes, 1)
	osc, node := iter.getMinReadingObjectSolution(allNodes, 1)
	if bsc < osc {
		logger.Debugf("downloading object from node %v(%v)", blocks[0].Node.Name, blocks[0].Node.NodeID)
		return iter.downloadFromNode(&blocks[0].Node, obj)
	}

	if osc == math.MaxFloat64 {
		// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
		return nil, fmt.Errorf("no node has this object")
	}

	logger.Debugf("downloading object from node %v(%v)", node.Name, node.NodeID)
	return iter.downloadFromNode(node, obj)
}

func (iter *DownloadObjectIterator) downloadECObject(req downloadReqeust2, ecRed *cdssdk.ECRedundancy) (io.ReadCloser, error) {
	allNodes, err := iter.sortDownloadNodes(req)
	if err != nil {
		return nil, err
	}

	bsc, blocks := iter.getMinReadingBlockSolution(allNodes, ecRed.K)
	osc, node := iter.getMinReadingObjectSolution(allNodes, ecRed.K)

	if bsc < osc {
		var logStrs []any = []any{"downloading ec object from blocks: "}
		for i, b := range blocks {
			if i > 0 {
				logStrs = append(logStrs, ", ")
			}
			logStrs = append(logStrs, fmt.Sprintf("%v@%v(%v)", b.Block.Index, b.Node.Name, b.Node.NodeID))
		}
		logger.Debug(logStrs...)

		pr, pw := io.Pipe()
		go func() {
			readPos := req.Raw.Offset
			totalReadLen := req.Detail.Object.Size - req.Raw.Offset
			if req.Raw.Length >= 0 {
				totalReadLen = math2.Min(req.Raw.Length, totalReadLen)
			}

			firstStripIndex := readPos / int64(ecRed.K) / int64(ecRed.ChunkSize)
			stripIter := NewStripIterator(req.Detail.Object, blocks, ecRed, firstStripIndex, iter.downloader.strips, iter.downloader.cfg.ECStripPrefetchCount)
			defer stripIter.Close()

			for totalReadLen > 0 {
				strip, err := stripIter.MoveNext()
				if err == iterator.ErrNoMoreItem {
					pw.CloseWithError(io.ErrUnexpectedEOF)
					return
				}
				if err != nil {
					pw.CloseWithError(err)
					return
				}

				readRelativePos := readPos - strip.Position
				nextStripPos := strip.Position + int64(ecRed.K)*int64(ecRed.ChunkSize)
				curReadLen := math2.Min(totalReadLen, nextStripPos-readPos)

				err = io2.WriteAll(pw, strip.Data[readRelativePos:readRelativePos+curReadLen])
				if err != nil {
					pw.CloseWithError(err)
					return
				}

				totalReadLen -= curReadLen
				readPos += curReadLen
			}
			pw.Close()
		}()

		return pr, nil
	}

	// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
	if osc == math.MaxFloat64 {
		return nil, fmt.Errorf("no enough blocks to reconstruct the file, want %d, get only %d", ecRed.K, len(blocks))
	}

	logger.Debugf("downloading ec object from node %v(%v)", node.Name, node.NodeID)
	return iter.downloadFromNode(node, req)
}

func (iter *DownloadObjectIterator) sortDownloadNodes(req downloadReqeust2) ([]*DownloadNodeInfo, error) {
	var nodeIDs []cdssdk.NodeID
	for _, id := range req.Detail.PinnedAt {
		if !lo.Contains(nodeIDs, id) {
			nodeIDs = append(nodeIDs, id)
		}
	}
	for _, b := range req.Detail.Blocks {
		if !lo.Contains(nodeIDs, b.NodeID) {
			nodeIDs = append(nodeIDs, b.NodeID)
		}
	}

	downloadNodeMap := make(map[cdssdk.NodeID]*DownloadNodeInfo)
	for _, id := range req.Detail.PinnedAt {
		node, ok := downloadNodeMap[id]
		if !ok {
			mod := iter.allNodes[id]
			node = &DownloadNodeInfo{
				Node:         mod,
				ObjectPinned: true,
				Distance:     iter.getNodeDistance(mod),
			}
			downloadNodeMap[id] = node
		}

		node.ObjectPinned = true
	}

	for _, b := range req.Detail.Blocks {
		node, ok := downloadNodeMap[b.NodeID]
		if !ok {
			mod := iter.allNodes[b.NodeID]
			node = &DownloadNodeInfo{
				Node:     mod,
				Distance: iter.getNodeDistance(mod),
			}
			downloadNodeMap[b.NodeID] = node
		}

		node.Blocks = append(node.Blocks, b)
	}

	return sort2.Sort(lo.Values(downloadNodeMap), func(left, right *DownloadNodeInfo) int {
		return sort2.Cmp(left.Distance, right.Distance)
	}), nil
}

func (iter *DownloadObjectIterator) getMinReadingBlockSolution(sortedNodes []*DownloadNodeInfo, k int) (float64, []downloadBlock) {
	gotBlocksMap := bitmap.Bitmap64(0)
	var gotBlocks []downloadBlock
	dist := float64(0.0)
	for _, n := range sortedNodes {
		for _, b := range n.Blocks {
			if !gotBlocksMap.Get(b.Index) {
				gotBlocks = append(gotBlocks, downloadBlock{
					Node:  n.Node,
					Block: b,
				})
				gotBlocksMap.Set(b.Index, true)
				dist += n.Distance
			}

			if len(gotBlocks) >= k {
				return dist, gotBlocks
			}
		}
	}

	return math.MaxFloat64, gotBlocks
}

func (iter *DownloadObjectIterator) getMinReadingObjectSolution(sortedNodes []*DownloadNodeInfo, k int) (float64, *cdssdk.Node) {
	dist := math.MaxFloat64
	var downloadNode *cdssdk.Node
	for _, n := range sortedNodes {
		if n.ObjectPinned && float64(k)*n.Distance < dist {
			dist = float64(k) * n.Distance
			downloadNode = &n.Node
		}
	}

	return dist, downloadNode
}

func (iter *DownloadObjectIterator) getNodeDistance(node cdssdk.Node) float64 {
	if stgglb.Local.NodeID != nil {
		if node.NodeID == *stgglb.Local.NodeID {
			return consts.NodeDistanceSameNode
		}
	}

	if node.LocationID == stgglb.Local.LocationID {
		return consts.NodeDistanceSameLocation
	}

	c := iter.downloader.conn.Get(node.NodeID)
	if c == nil || c.Delay == nil || *c.Delay > time.Duration(float64(time.Millisecond)*iter.downloader.cfg.HighLatencyNodeMs) {
		return consts.NodeDistanceHighLatencyNode
	}

	return consts.NodeDistanceOther
}

func (iter *DownloadObjectIterator) downloadFromNode(node *cdssdk.Node, req downloadReqeust2) (io.ReadCloser, error) {
	var strHandle *plans.ExecutorReadStream
	ft := plans.NewFromTo()

	toExec, handle := plans.NewToExecutor(-1)
	toExec.Range = plans.Range{
		Offset: req.Raw.Offset,
	}
	if req.Raw.Length != -1 {
		len := req.Raw.Length
		toExec.Range.Length = &len
	}
	ft.AddFrom(plans.NewFromNode(req.Detail.Object.FileHash, node, -1)).AddTo(toExec)
	strHandle = handle

	parser := plans.NewParser(cdssdk.DefaultECRedundancy)
	plans := plans.NewPlanBuilder()
	if err := parser.Parse(ft, plans); err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	exec := plans.Execute()
	go exec.Wait(context.TODO())

	return exec.BeginRead(strHandle)
}
