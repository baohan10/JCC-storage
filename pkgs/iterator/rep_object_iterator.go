package iterator

import (
	"fmt"
	"io"
	"math/rand"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock/reqbuilder"
	distsvc "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	"gitlink.org.cn/cloudream/storage-common/globals"
	"gitlink.org.cn/cloudream/storage-common/models"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

type DownloadingObjectIterator = Iterator[*IterDownloadingObject]

type RepObjectIterator struct {
	objects       []model.Object
	objectRepData []models.ObjectRepData
	currentIndex  int
	inited        bool

	downloadCtx *DownloadContext
	cliLocation model.Location
}

type IterDownloadingObject struct {
	Object model.Object
	File   io.ReadCloser
}

type DownloadNodeInfo struct {
	Node           model.Node
	IsSameLocation bool
}

type DownloadContext struct {
	Distlock *distsvc.Service
}

func NewRepObjectIterator(objects []model.Object, objectRepData []models.ObjectRepData, downloadCtx *DownloadContext) *RepObjectIterator {
	return &RepObjectIterator{
		objects:       objects,
		objectRepData: objectRepData,
		downloadCtx:   downloadCtx,
	}
}

func (i *RepObjectIterator) MoveNext() (*IterDownloadingObject, error) {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

	if !i.inited {
		i.inited = true

		findCliLocResp, err := coorCli.FindClientLocation(coormq.NewFindClientLocation(globals.Local.ExternalIP))
		if err != nil {
			return nil, fmt.Errorf("finding client location: %w", err)
		}
		i.cliLocation = findCliLocResp.Location
	}

	if i.currentIndex >= len(i.objects) {
		return nil, ErrNoMoreItem
	}

	item, err := i.doMove(coorCli)
	i.currentIndex++
	return item, err
}

func (i *RepObjectIterator) doMove(coorCli *coormq.PoolClient) (*IterDownloadingObject, error) {
	repData := i.objectRepData[i.currentIndex]
	if len(repData.NodeIDs) == 0 {
		return nil, fmt.Errorf("no node has this file %s", repData.FileHash)
	}

	getNodesResp, err := coorCli.GetNodes(coormq.NewGetNodes(repData.NodeIDs))
	if err != nil {
		return nil, fmt.Errorf("getting nodes: %w", err)
	}

	downloadNodes := lo.Map(getNodesResp.Nodes, func(node model.Node, index int) DownloadNodeInfo {
		return DownloadNodeInfo{
			Node:           node,
			IsSameLocation: node.LocationID == i.cliLocation.LocationID,
		}
	})

	// 选择下载节点
	downloadNode := i.chooseDownloadNode(downloadNodes)

	// 如果客户端与节点在同一个地域，则使用内网地址连接节点
	nodeIP := downloadNode.Node.ExternalIP
	if downloadNode.IsSameLocation {
		nodeIP = downloadNode.Node.LocalIP

		logger.Infof("client and node %d are at the same location, use local ip\n", downloadNode.Node.NodeID)
	}

	reader, err := downloadFile(i.downloadCtx, downloadNode.Node.NodeID, nodeIP, repData.FileHash)
	if err != nil {
		return nil, fmt.Errorf("rep read failed, err: %w", err)
	}
	return &IterDownloadingObject{
		Object: i.objects[i.currentIndex],
		File:   reader,
	}, nil
}

func (i *RepObjectIterator) Close() {

}

// chooseDownloadNode 选择一个下载节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (i *RepObjectIterator) chooseDownloadNode(entries []DownloadNodeInfo) DownloadNodeInfo {
	sameLocationEntries := lo.Filter(entries, func(e DownloadNodeInfo, i int) bool { return e.IsSameLocation })
	if len(sameLocationEntries) > 0 {
		return sameLocationEntries[rand.Intn(len(sameLocationEntries))]
	}

	return entries[rand.Intn(len(entries))]
}

func downloadFile(ctx *DownloadContext, nodeID int64, nodeIP string, fileHash string) (io.ReadCloser, error) {
	if globals.IPFSPool != nil {
		logger.Infof("try to use local IPFS to download file")

		reader, err := downloadFromLocalIPFS(fileHash)
		if err == nil {
			return reader, nil
		}

		logger.Warnf("download from local IPFS failed, so try to download from node %s, err: %s", nodeIP, err.Error())
	}

	return downloadFromNode(ctx, nodeID, nodeIP, fileHash)
}

func downloadFromNode(ctx *DownloadContext, nodeID int64, nodeIP string, fileHash string) (io.ReadCloser, error) {
	// 二次获取锁
	mutex, err := reqbuilder.NewBuilder().
		// 用于从IPFS下载文件
		IPFS().ReadOneRep(nodeID, fileHash).
		MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}

	// 连接grpc
	agtCli, err := globals.AgentRPCPool.Acquire(nodeIP)
	if err != nil {
		return nil, fmt.Errorf("new agent grpc client: %w", err)
	}

	reader, err := agtCli.GetIPFSFile(fileHash)
	if err != nil {
		return nil, fmt.Errorf("getting ipfs file: %w", err)
	}

	reader = myio.AfterReadClosed(reader, func(io.ReadCloser) {
		mutex.Unlock()
	})
	return reader, nil
}

func downloadFromLocalIPFS(fileHash string) (io.ReadCloser, error) {
	ipfsCli, err := globals.IPFSPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new ipfs client: %w", err)
	}

	reader, err := ipfsCli.OpenRead(fileHash)
	if err != nil {
		return nil, fmt.Errorf("read ipfs file failed, err: %w", err)
	}

	return reader, nil
}
