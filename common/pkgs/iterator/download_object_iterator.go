package iterator

import (
	"fmt"
	"io"
	"math/rand"
	"reflect"

	"github.com/samber/lo"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	myio "gitlink.org.cn/cloudream/common/utils/io"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
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
	Node           cdssdk.Node
	IsSameLocation bool
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

// chooseDownloadNode 选择一个下载节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (i *DownloadObjectIterator) chooseDownloadNode(entries []DownloadNodeInfo) DownloadNodeInfo {
	sameLocationEntries := lo.Filter(entries, func(e DownloadNodeInfo, i int) bool { return e.IsSameLocation })
	if len(sameLocationEntries) > 0 {
		return sameLocationEntries[rand.Intn(len(sameLocationEntries))]
	}

	return entries[rand.Intn(len(entries))]
}

func (iter *DownloadObjectIterator) downloadNoneOrRepObject(coorCli *coormq.Client, ctx *DownloadContext, obj stgmodels.ObjectDetail) (io.ReadCloser, error) {
	if len(obj.Blocks) == 0 {
		return nil, fmt.Errorf("no node has this object")
	}

	//采取直接读，优先选内网节点
	var chosenNodes []DownloadNodeInfo

	grpBlocks := obj.GroupBlocks()
	for _, grp := range grpBlocks {
		getNodesResp, err := coorCli.GetNodes(coormq.NewGetNodes(grp.NodeIDs))
		if err != nil {
			continue
		}

		downloadNodes := lo.Map(getNodesResp.Nodes, func(node cdssdk.Node, index int) DownloadNodeInfo {
			return DownloadNodeInfo{
				Node:           node,
				IsSameLocation: node.LocationID == stgglb.Local.LocationID,
			}
		})

		chosenNodes = append(chosenNodes, iter.chooseDownloadNode(downloadNodes))
	}

	var fileStrs []io.ReadCloser

	for i := range grpBlocks {
		str, err := downloadFile(ctx, chosenNodes[i], grpBlocks[i].FileHash)
		if err != nil {
			for i -= 1; i >= 0; i-- {
				fileStrs[i].Close()
			}
			return nil, fmt.Errorf("donwloading file: %w", err)
		}

		fileStrs = append(fileStrs, str)
	}

	fileReaders, filesCloser := myio.ToReaders(fileStrs)
	return myio.AfterReadClosed(myio.Length(myio.Join(fileReaders), obj.Object.Size), func(c io.ReadCloser) {
		filesCloser()
	}), nil
}

func (iter *DownloadObjectIterator) downloadECObject(coorCli *coormq.Client, ctx *DownloadContext, obj stgmodels.ObjectDetail, ecRed *cdssdk.ECRedundancy) (io.ReadCloser, error) {
	//采取直接读，优先选内网节点
	var chosenNodes []DownloadNodeInfo
	var chosenBlocks []stgmodels.GrouppedObjectBlock
	grpBlocks := obj.GroupBlocks()
	for i := range grpBlocks {
		if len(chosenBlocks) == ecRed.K {
			break
		}

		getNodesResp, err := coorCli.GetNodes(coormq.NewGetNodes(grpBlocks[i].NodeIDs))
		if err != nil {
			continue
		}

		downloadNodes := lo.Map(getNodesResp.Nodes, func(node cdssdk.Node, index int) DownloadNodeInfo {
			return DownloadNodeInfo{
				Node:           node,
				IsSameLocation: node.LocationID == stgglb.Local.LocationID,
			}
		})

		chosenBlocks = append(chosenBlocks, grpBlocks[i])
		chosenNodes = append(chosenNodes, iter.chooseDownloadNode(downloadNodes))

	}

	if len(chosenBlocks) < ecRed.K {
		return nil, fmt.Errorf("no enough blocks to reconstruct the file, want %d, get only %d", ecRed.K, len(chosenBlocks))
	}

	var fileStrs []io.ReadCloser

	rs, err := ec.NewRs(ecRed.K, ecRed.N, ecRed.ChunkSize)
	if err != nil {
		return nil, fmt.Errorf("new rs: %w", err)
	}

	for i := range chosenBlocks {
		str, err := downloadFile(ctx, chosenNodes[i], chosenBlocks[i].FileHash)
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
	for _, b := range chosenBlocks {
		indexes = append(indexes, b.Index)
	}

	outputs, outputsCloser := myio.ToReaders(rs.ReconstructData(fileReaders, indexes))
	return myio.AfterReadClosed(myio.Length(myio.ChunkedJoin(outputs, int(ecRed.ChunkSize)), obj.Object.Size), func(c io.ReadCloser) {
		filesCloser()
		outputsCloser()
	}), nil
}

func downloadFile(ctx *DownloadContext, node DownloadNodeInfo, fileHash string) (io.ReadCloser, error) {
	// 如果客户端与节点在同一个地域，则使用内网地址连接节点
	nodeIP := node.Node.ExternalIP
	grpcPort := node.Node.ExternalGRPCPort
	if node.IsSameLocation {
		nodeIP = node.Node.LocalIP
		grpcPort = node.Node.LocalGRPCPort

		logger.Infof("client and node %d are at the same location, use local ip", node.Node.NodeID)
	}

	if stgglb.IPFSPool != nil {
		logger.Infof("try to use local IPFS to download file")

		reader, err := downloadFromLocalIPFS(ctx, fileHash)
		if err == nil {
			return reader, nil
		}

		logger.Warnf("download from local IPFS failed, so try to download from node %s, err: %s", nodeIP, err.Error())
	}

	return downloadFromNode(ctx, node.Node.NodeID, nodeIP, grpcPort, fileHash)
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
