package iterator

import (
	"fmt"
	"io"
	"math"
	"reflect"

	"github.com/samber/lo"

	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	myio "gitlink.org.cn/cloudream/common/utils/io"
	mysort "gitlink.org.cn/cloudream/common/utils/sort"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	stgmodels "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type DownloadingObjectIterator = Iterator[*IterDownloadingObject]

type IterDownloadingObject struct {
	Object model.Object
	File   io.ReadCloser
}

type DownloadNodeInfo struct {
	Node         model.Node
	ObjectPinned bool
	Blocks       []stgmod.ObjectBlock
	Distance     float64
}

type DownloadContext struct {
	Distlock *distlock.Service
}
type DownloadObjectIterator struct {
	OnClosing func()

	objectDetails []stgmodels.ObjectDetail
	currentIndex  int

	downloadCtx *DownloadContext
}

func NewDownloadObjectIterator(objectDetails []stgmodels.ObjectDetail, downloadCtx *DownloadContext) *DownloadObjectIterator {
	return &DownloadObjectIterator{
		objectDetails: objectDetails,
		downloadCtx:   downloadCtx,
	}
}

func (i *DownloadObjectIterator) MoveNext() (*IterDownloadingObject, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	if i.currentIndex >= len(i.objectDetails) {
		return nil, ErrNoMoreItem
	}

	item, err := i.doMove(coorCli)
	i.currentIndex++
	return item, err
}

func (iter *DownloadObjectIterator) doMove(coorCli *coormq.Client) (*IterDownloadingObject, error) {
	obj := iter.objectDetails[iter.currentIndex]

	switch red := obj.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		reader, err := iter.downloadNoneOrRepObject(coorCli, iter.downloadCtx, obj)
		if err != nil {
			return nil, fmt.Errorf("downloading object: %w", err)
		}

		return &IterDownloadingObject{
			Object: obj.Object,
			File:   reader,
		}, nil

	case *cdssdk.RepRedundancy:
		reader, err := iter.downloadNoneOrRepObject(coorCli, iter.downloadCtx, obj)
		if err != nil {
			return nil, fmt.Errorf("downloading rep object: %w", err)
		}

		return &IterDownloadingObject{
			Object: obj.Object,
			File:   reader,
		}, nil

	case *cdssdk.ECRedundancy:
		reader, err := iter.downloadECObject(coorCli, iter.downloadCtx, obj, red)
		if err != nil {
			return nil, fmt.Errorf("downloading ec object: %w", err)
		}

		return &IterDownloadingObject{
			Object: obj.Object,
			File:   reader,
		}, nil
	}

	return nil, fmt.Errorf("unsupported redundancy type: %v", reflect.TypeOf(obj.Object.Redundancy))
}

func (i *DownloadObjectIterator) Close() {
	if i.OnClosing != nil {
		i.OnClosing()
	}
}

func (iter *DownloadObjectIterator) downloadNoneOrRepObject(coorCli *coormq.Client, ctx *DownloadContext, obj stgmodels.ObjectDetail) (io.ReadCloser, error) {
	allNodes, err := iter.sortDownloadNodes(coorCli, ctx, obj)
	if err != nil {
		return nil, err
	}
	bsc, blocks := iter.getMinReadingBlockSolution(allNodes, 1)
	osc, node := iter.getMinReadingObjectSolution(allNodes, 1)
	if bsc < osc {
		return downloadFile(ctx, blocks[0].Node, blocks[0].Block.FileHash)
	}

	// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
	if osc == math.MaxFloat64 {
		return nil, fmt.Errorf("no node has this object")
	}

	return downloadFile(ctx, *node, obj.Object.FileHash)
}

func (iter *DownloadObjectIterator) downloadECObject(coorCli *coormq.Client, ctx *DownloadContext, obj stgmodels.ObjectDetail, ecRed *cdssdk.ECRedundancy) (io.ReadCloser, error) {
	allNodes, err := iter.sortDownloadNodes(coorCli, ctx, obj)
	if err != nil {
		return nil, err
	}
	bsc, blocks := iter.getMinReadingBlockSolution(allNodes, ecRed.K)
	osc, node := iter.getMinReadingObjectSolution(allNodes, ecRed.K)
	if bsc < osc {
		var fileStrs []io.ReadCloser

		rs, err := ec.NewRs(ecRed.K, ecRed.N, ecRed.ChunkSize)
		if err != nil {
			return nil, fmt.Errorf("new rs: %w", err)
		}

		for i, b := range blocks {
			str, err := downloadFile(ctx, b.Node, b.Block.FileHash)
			if err != nil {
				for i -= 1; i >= 0; i-- {
					fileStrs[i].Close()
				}
				return nil, fmt.Errorf("donwloading file: %w", err)
			}

			fileStrs = append(fileStrs, str)
		}

		fileReaders, filesCloser := myio.ToReaders(fileStrs)

		var indexes []int
		for _, b := range blocks {
			indexes = append(indexes, b.Block.Index)
		}

		outputs, outputsCloser := myio.ToReaders(rs.ReconstructData(fileReaders, indexes))
		return myio.AfterReadClosed(myio.Length(myio.ChunkedJoin(outputs, int(ecRed.ChunkSize)), obj.Object.Size), func(c io.ReadCloser) {
			filesCloser()
			outputsCloser()
		}), nil
	}

	// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
	if osc == math.MaxFloat64 {
		return nil, fmt.Errorf("no enough blocks to reconstruct the file, want %d, get only %d", ecRed.K, len(blocks))
	}

	return downloadFile(ctx, *node, obj.Object.FileHash)
}

func (iter *DownloadObjectIterator) sortDownloadNodes(coorCli *coormq.Client, ctx *DownloadContext, obj stgmodels.ObjectDetail) ([]*DownloadNodeInfo, error) {
	var nodeIDs []cdssdk.NodeID
	for _, id := range obj.PinnedAt {
		if !lo.Contains(nodeIDs, id) {
			nodeIDs = append(nodeIDs, id)
		}
	}
	for _, b := range obj.Blocks {
		if !lo.Contains(nodeIDs, b.NodeID) {
			nodeIDs = append(nodeIDs, b.NodeID)
		}
	}

	getNodes, err := coorCli.GetNodes(coormq.NewGetNodes(nodeIDs))
	if err != nil {
		return nil, fmt.Errorf("getting nodes: %w", err)
	}

	downloadNodeMap := make(map[cdssdk.NodeID]*DownloadNodeInfo)
	for _, id := range obj.PinnedAt {
		node, ok := downloadNodeMap[id]
		if !ok {
			mod := *getNodes.GetNode(id)
			node = &DownloadNodeInfo{
				Node:         mod,
				ObjectPinned: true,
				Distance:     iter.getNodeDistance(mod),
			}
			downloadNodeMap[id] = node
		}

		node.ObjectPinned = true
	}

	for _, b := range obj.Blocks {
		node, ok := downloadNodeMap[b.NodeID]
		if !ok {
			mod := *getNodes.GetNode(b.NodeID)
			node = &DownloadNodeInfo{
				Node:     mod,
				Distance: iter.getNodeDistance(mod),
			}
			downloadNodeMap[b.NodeID] = node
		}

		node.Blocks = append(node.Blocks, b)
	}

	return mysort.Sort(lo.Values(downloadNodeMap), func(left, right *DownloadNodeInfo) int {
		return mysort.Cmp(left.Distance, right.Distance)
	}), nil
}

type downloadBlock struct {
	Node  model.Node
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

func (iter *DownloadObjectIterator) getMinReadingObjectSolution(sortedNodes []*DownloadNodeInfo, k int) (float64, *model.Node) {
	dist := math.MaxFloat64
	var downloadNode *model.Node
	for _, n := range sortedNodes {
		if n.ObjectPinned && float64(k)*n.Distance < dist {
			dist = float64(k) * n.Distance
			downloadNode = &n.Node
		}
	}

	return dist, downloadNode
}

func (iter *DownloadObjectIterator) getNodeDistance(node model.Node) float64 {
	if stgglb.Local.NodeID != nil {
		if node.NodeID == *stgglb.Local.NodeID {
			return consts.NodeDistanceSameNode
		}
	}

	if node.LocationID == stgglb.Local.LocationID {
		return consts.NodeDistanceSameLocation
	}

	return consts.NodeDistanceOther
}

func downloadFile(ctx *DownloadContext, node model.Node, fileHash string) (io.ReadCloser, error) {
	// 如果客户端与节点在同一个地域，则使用内网地址连接节点
	nodeIP := node.ExternalIP
	grpcPort := node.ExternalGRPCPort
	if node.LocationID == stgglb.Local.LocationID {
		nodeIP = node.LocalIP
		grpcPort = node.LocalGRPCPort

		logger.Infof("client and node %d are at the same location, use local ip", node.NodeID)
	}

	if stgglb.IPFSPool != nil {
		logger.Infof("try to use local IPFS to download file")

		reader, err := downloadFromLocalIPFS(ctx, fileHash)
		if err == nil {
			return reader, nil
		}

		logger.Warnf("download from local IPFS failed, so try to download from node %s, err: %s", nodeIP, err.Error())
	}

	return downloadFromNode(ctx, node.NodeID, nodeIP, grpcPort, fileHash)
}

func downloadFromNode(ctx *DownloadContext, nodeID cdssdk.NodeID, nodeIP string, grpcPort int, fileHash string) (io.ReadCloser, error) {
	agtCli, err := stgglb.AgentRPCPool.Acquire(nodeIP, grpcPort)
	if err != nil {
		return nil, fmt.Errorf("new agent grpc client: %w", err)
	}

	reader, err := agtCli.GetIPFSFile(fileHash)
	if err != nil {
		return nil, fmt.Errorf("getting ipfs file: %w", err)
	}

	reader = myio.AfterReadClosed(reader, func(io.ReadCloser) {
		agtCli.Close()
	})
	return reader, nil
}

func downloadFromLocalIPFS(ctx *DownloadContext, fileHash string) (io.ReadCloser, error) {
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new ipfs client: %w", err)
	}

	reader, err := ipfsCli.OpenRead(fileHash)
	if err != nil {
		return nil, fmt.Errorf("read ipfs file failed, err: %w", err)
	}

	reader = myio.AfterReadClosed(reader, func(io.ReadCloser) {
		ipfsCli.Close()
	})
	return reader, nil
}
