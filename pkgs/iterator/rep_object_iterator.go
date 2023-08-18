package iterator

import (
	"fmt"
	"io"
	"math/rand"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock/reqbuilder"
	distsvc "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	"gitlink.org.cn/cloudream/storage-common/models"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	mymq "gitlink.org.cn/cloudream/storage-common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	myio "gitlink.org.cn/cloudream/common/utils/io"
	agentcaller "gitlink.org.cn/cloudream/storage-common/pkgs/proto"
	mygrpc "gitlink.org.cn/cloudream/storage-common/utils/grpc"
)

type DownloadingObjectIterator = Iterator[*IterDownloadingObject]

type RepObjectIterator struct {
	objects       []model.Object
	objectRepData []models.ObjectRepData
	currentIndex  int
	inited        bool

	coorCli        *coormq.Client
	distlock       *distsvc.Service
	downloadConfig DownloadConfig
	cliLocation    model.Location
}

type IterDownloadingObject struct {
	Object model.Object
	File   io.ReadCloser
}

type DownloadNodeInfo struct {
	Node           model.Node
	IsSameLocation bool
}

type DownloadConfig struct {
	LocalIPFS   *ipfs.IPFS
	LocalNodeID *int64
	ExternalIP  string
	GRPCPort    int
	MQ          *mymq.Config
}

func NewRepObjectIterator(objects []model.Object, objectRepData []models.ObjectRepData, coorCli *coormq.Client, distlock *distsvc.Service, downloadConfig DownloadConfig) *RepObjectIterator {
	return &RepObjectIterator{
		objects:        objects,
		objectRepData:  objectRepData,
		coorCli:        coorCli,
		distlock:       distlock,
		downloadConfig: downloadConfig,
	}
}

func (i *RepObjectIterator) MoveNext() (*IterDownloadingObject, error) {
	if !i.inited {
		i.inited = true

		findCliLocResp, err := i.coorCli.FindClientLocation(coormq.NewFindClientLocation(i.downloadConfig.ExternalIP))
		if err != nil {
			return nil, fmt.Errorf("finding client location: %w", err)
		}
		i.cliLocation = findCliLocResp.Location
	}

	if i.currentIndex >= len(i.objects) {
		return nil, ErrNoMoreItem
	}

	item, err := i.doMove()
	i.currentIndex++
	return item, err
}

func (i *RepObjectIterator) doMove() (*IterDownloadingObject, error) {
	repData := i.objectRepData[i.currentIndex]
	if len(repData.NodeIDs) == 0 {
		return nil, fmt.Errorf("no node has this file %s", repData.FileHash)
	}

	getNodesResp, err := i.coorCli.GetNodes(coormq.NewGetNodes(repData.NodeIDs))
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

	reader, err := i.downloadObject(downloadNode.Node.NodeID, nodeIP, repData.FileHash)
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

func (i *RepObjectIterator) downloadObject(nodeID int64, nodeIP string, fileHash string) (io.ReadCloser, error) {
	if i.downloadConfig.LocalIPFS != nil {
		logger.Infof("try to use local IPFS to download file")

		reader, err := i.downloadFromLocalIPFS(fileHash)
		if err == nil {
			return reader, nil
		}

		logger.Warnf("download from local IPFS failed, so try to download from node %s, err: %s", nodeIP, err.Error())
	}

	return i.downloadFromNode(nodeID, nodeIP, fileHash)
}

func (i *RepObjectIterator) downloadFromNode(nodeID int64, nodeIP string, fileHash string) (io.ReadCloser, error) {
	// 二次获取锁
	mutex, err := reqbuilder.NewBuilder().
		// 用于从IPFS下载文件
		IPFS().ReadOneRep(nodeID, fileHash).
		MutexLock(i.distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}

	// 连接grpc
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, i.downloadConfig.GRPCPort)
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}

	// 下载文件
	client := agentcaller.NewFileTransportClient(conn)
	reader, err := mygrpc.GetFileAsStream(client, fileHash)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("request to get file failed, err: %w", err)
	}

	reader = myio.AfterReadClosed(reader, func(io.ReadCloser) {
		conn.Close()
		mutex.Unlock()
	})
	return reader, nil
}

func (i *RepObjectIterator) downloadFromLocalIPFS(fileHash string) (io.ReadCloser, error) {
	reader, err := i.downloadConfig.LocalIPFS.OpenRead(fileHash)
	if err != nil {
		return nil, fmt.Errorf("read ipfs file failed, err: %w", err)
	}

	return reader, nil
}
