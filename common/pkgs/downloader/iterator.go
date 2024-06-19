package downloader

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/samber/lo"

	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/common/utils/sync2"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
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

		return NewIPFSReaderWithRange(blocks[0].Node, blocks[0].Block.FileHash, ipfs.ReadOption{
			Offset: obj.Raw.Offset,
			Length: obj.Raw.Length,
		}), nil
	}

	// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
	if osc == math.MaxFloat64 {
		return nil, fmt.Errorf("no node has this object")
	}

	logger.Debugf("downloading object from node %v(%v)", node.Name, node.NodeID)
	return NewIPFSReaderWithRange(*node, obj.Detail.Object.FileHash, ipfs.ReadOption{
		Offset: obj.Raw.Offset,
		Length: obj.Raw.Length,
	}), nil
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
			logStrs = append(logStrs, fmt.Sprintf("%v: %v(%v)", b.Block.Index, b.Node.Name, b.Node.NodeID))
		}
		logger.Debug(logStrs...)

		var fileStrs []*IPFSReader
		for _, b := range blocks {
			str := NewIPFSReader(b.Node, b.Block.FileHash)

			fileStrs = append(fileStrs, str)
		}

		rs, err := ec.NewRs(ecRed.K, ecRed.N)
		if err != nil {
			return nil, fmt.Errorf("new rs: %w", err)
		}

		pr, pw := io.Pipe()
		go func() {
			defer func() {
				for _, str := range fileStrs {
					str.Close()
				}
			}()

			readPos := req.Raw.Offset
			totalReadLen := req.Detail.Object.Size - req.Raw.Offset
			if req.Raw.Length >= 0 {
				totalReadLen = math2.Min(req.Raw.Length, totalReadLen)
			}

			for totalReadLen > 0 {
				curStripPos := readPos / int64(ecRed.K) / int64(ecRed.ChunkSize)
				curStripPosInBytes := curStripPos * int64(ecRed.K) * int64(ecRed.ChunkSize)
				nextStripPosInBytes := (curStripPos + 1) * int64(ecRed.K) * int64(ecRed.ChunkSize)
				curReadLen := math2.Min(totalReadLen, nextStripPosInBytes-readPos)
				readRelativePos := readPos - curStripPosInBytes
				cacheKey := ECStripKey{
					ObjectID:      req.Detail.Object.ObjectID,
					StripPosition: curStripPos,
				}

				cache, ok := iter.downloader.strips.Get(cacheKey)
				if ok {
					if cache.ObjectFileHash == req.Detail.Object.FileHash {
						err := io2.WriteAll(pw, cache.Data[readRelativePos:readRelativePos+curReadLen])
						if err != nil {
							pw.CloseWithError(err)
							return
						}
						totalReadLen -= curReadLen
						readPos += curReadLen
						continue
					}

					// 如果Object的Hash和Cache的Hash不一致，说明Cache是无效的，需要重新下载
					iter.downloader.strips.Remove(cacheKey)
				}
				for _, str := range fileStrs {
					_, err := str.Seek(curStripPos*int64(ecRed.ChunkSize), io.SeekStart)
					if err != nil {
						pw.CloseWithError(err)
						return
					}
				}

				dataBuf := make([]byte, int64(ecRed.K*ecRed.ChunkSize))
				blockArrs := make([][]byte, ecRed.N)
				for i := 0; i < ecRed.K; i++ {
					// 放入的slice长度为0，但容量为ChunkSize，EC库发现长度为0的块后才会认为是待恢复块
					blockArrs[i] = dataBuf[i*ecRed.ChunkSize : i*ecRed.ChunkSize]
				}
				for _, b := range blocks {
					// 用于恢复的块则要将其长度变回ChunkSize，用于后续读取块数据
					if b.Block.Index < ecRed.K {
						// 此处扩容不会导致slice指向一个新内存
						blockArrs[b.Block.Index] = blockArrs[b.Block.Index][0:ecRed.ChunkSize]
					} else {
						blockArrs[b.Block.Index] = make([]byte, ecRed.ChunkSize)
					}
				}

				err := sync2.ParallelDo(blocks, func(b downloadBlock, idx int) error {
					_, err := io.ReadFull(fileStrs[idx], blockArrs[b.Block.Index])
					return err
				})
				if err != nil {
					pw.CloseWithError(err)
					return
				}

				err = rs.ReconstructData(blockArrs)
				if err != nil {
					pw.CloseWithError(err)
					return
				}

				iter.downloader.strips.Add(cacheKey, ObjectECStrip{
					Data:           dataBuf,
					ObjectFileHash: req.Detail.Object.FileHash,
				})
				// 下次循环就能从Cache中读取数据
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
	return NewIPFSReaderWithRange(*node, req.Detail.Object.FileHash, ipfs.ReadOption{
		Offset: req.Raw.Offset,
		Length: req.Raw.Length,
	}), nil
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

type downloadBlock struct {
	Node  cdssdk.Node
	Block stgmod.ObjectBlock
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
