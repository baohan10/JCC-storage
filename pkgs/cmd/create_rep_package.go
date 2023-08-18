package cmd

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	mygrpc "gitlink.org.cn/cloudream/storage-common/utils/grpc"

	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
	mymq "gitlink.org.cn/cloudream/storage-common/pkgs/mq"
	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
	agentcaller "gitlink.org.cn/cloudream/storage-common/pkgs/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type UploadNodeInfo struct {
	Node           model.Node
	IsSameLocation bool
}

type CreateRepPackage struct {
	userID       int64
	bucketID     int64
	name         string
	objectIter   iterator.UploadingObjectIterator
	redundancy   models.RepRedundancyInfo
	uploadConfig UploadConfig

	Result CreateRepPackageResult
}

type UploadConfig struct {
	LocalIPFS   *ipfs.IPFS
	LocalNodeID *int64
	ExternalIP  string
	GRPCPort    int
	MQ          *mymq.Config
}

type CreateRepPackageResult struct {
	PackageID     int64
	ObjectResults []RepObjectUploadResult
}

type RepObjectUploadResult struct {
	Info     *iterator.IterUploadingObject
	Error    error
	FileHash string
	ObjectID int64
}

func NewCreateRepPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, redundancy models.RepRedundancyInfo, uploadConfig UploadConfig) *CreateRepPackage {
	return &CreateRepPackage{
		userID:       userID,
		bucketID:     bucketID,
		name:         name,
		objectIter:   objIter,
		redundancy:   redundancy,
		uploadConfig: uploadConfig,
	}
}

func (t *CreateRepPackage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	t.objectIter.Close()
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *CreateRepPackage) do(ctx TaskContext) error {
	/*
		// TODO2
		reqBlder := reqbuilder.NewBuilder()
		for _, uploadObject := range t.Objects {
			reqBlder.Metadata().
				// 用于防止创建了多个同名对象
				Object().CreateOne(t.bucketID, uploadObject.ObjectName)
		}

		// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
		if t.uploadConfig.LocalNodeID != nil {
			reqBlder.IPFS().CreateAnyRep(*t.uploadConfig.LocalNodeID)
		}

		mutex, err := reqBlder.
			Metadata().
			// 用于判断用户是否有桶的权限
			UserBucket().ReadOne(t.userID, t.bucketID).
			// 用于查询可用的上传节点
			Node().ReadAny().
			// 用于设置Rep配置
			ObjectRep().CreateAny().
			// 用于创建Cache记录
			Cache().CreateAny().
			MutexLock(ctx.DistLock())
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()
	*/
	createPkgResp, err := ctx.Coordinator().CreatePackage(coormq.NewCreatePackage(t.userID, t.bucketID, t.name,
		models.NewTypedRedundancyInfo(models.RedundancyRep, t.redundancy)))
	if err != nil {
		return fmt.Errorf("creating package: %w", err)
	}

	getUserNodesResp, err := ctx.Coordinator().GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return fmt.Errorf("getting user nodes: %w", err)
	}

	findCliLocResp, err := ctx.Coordinator().FindClientLocation(coormq.NewFindClientLocation(t.uploadConfig.ExternalIP))
	if err != nil {
		return fmt.Errorf("finding client location: %w", err)
	}

	nodeInfos := lo.Map(getUserNodesResp.Nodes, func(node model.Node, index int) UploadNodeInfo {
		return UploadNodeInfo{
			Node:           node,
			IsSameLocation: node.LocationID == findCliLocResp.Location.LocationID,
		}
	})
	uploadNode := t.chooseUploadNode(nodeInfos)

	// 防止上传的副本被清除
	mutex2, err := reqbuilder.NewBuilder().
		IPFS().CreateAnyRep(uploadNode.Node.NodeID).
		MutexLock(ctx.DistLock())
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex2.Unlock()

	rets, err := uploadAndUpdateRepPackage(ctx, createPkgResp.PackageID, t.objectIter, uploadNode, t.uploadConfig)
	if err != nil {
		return err
	}

	t.Result.PackageID = createPkgResp.PackageID
	t.Result.ObjectResults = rets
	return nil
}

func uploadAndUpdateRepPackage(ctx TaskContext, packageID int64, objectIter iterator.UploadingObjectIterator, uploadNode UploadNodeInfo, uploadConfig UploadConfig) ([]RepObjectUploadResult, error) {
	var uploadRets []RepObjectUploadResult
	//上传文件夹
	var adds []coormq.AddRepObjectInfo
	for {
		objInfo, err := objectIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading object: %w", err)
		}

		fileHash, uploadedNodeIDs, err := uploadObject(ctx, objInfo, uploadNode, uploadConfig)
		uploadRets = append(uploadRets, RepObjectUploadResult{
			Info:     objInfo,
			Error:    err,
			FileHash: fileHash,
		})
		if err != nil {
			return nil, fmt.Errorf("uploading object: %w", err)
		}

		adds = append(adds, coormq.NewAddRepObjectInfo(objInfo.Path, objInfo.Size, fileHash, uploadedNodeIDs))
	}

	_, err := ctx.Coordinator().UpdateRepPackage(coormq.NewUpdateRepPackage(packageID, adds, nil))
	if err != nil {
		return nil, fmt.Errorf("updating package: %w", err)
	}

	return uploadRets, nil
}

// 上传文件
func uploadObject(ctx TaskContext, obj *iterator.IterUploadingObject, uploadNode UploadNodeInfo, uploadConfig UploadConfig) (string, []int64, error) {
	// 本地有IPFS，则直接从本地IPFS上传
	if uploadConfig.LocalIPFS != nil {
		logger.Infof("try to use local IPFS to upload file")

		// 只有本地IPFS不是存储系统中的一个节点，才需要Pin文件
		fileHash, err := uploadToLocalIPFS(uploadConfig.LocalIPFS, obj.File, uploadNode.Node.NodeID, uploadConfig.LocalNodeID == nil, uploadConfig)
		if err == nil {
			return fileHash, []int64{*uploadConfig.LocalNodeID}, nil

		} else {
			logger.Warnf("upload to local IPFS failed, so try to upload to node %d, err: %s", uploadNode.Node.NodeID, err.Error())
		}
	}

	// 否则发送到agent上传
	// 如果客户端与节点在同一个地域，则使用内网地址连接节点
	nodeIP := uploadNode.Node.ExternalIP
	if uploadNode.IsSameLocation {
		nodeIP = uploadNode.Node.LocalIP

		logger.Infof("client and node %d are at the same location, use local ip\n", uploadNode.Node.NodeID)
	}

	fileHash, err := uploadToNode(obj.File, nodeIP, uploadConfig)
	if err != nil {
		return "", nil, fmt.Errorf("upload to node %s failed, err: %w", nodeIP, err)
	}

	return fileHash, []int64{uploadNode.Node.NodeID}, nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (t *CreateRepPackage) chooseUploadNode(nodes []UploadNodeInfo) UploadNodeInfo {
	sameLocationNodes := lo.Filter(nodes, func(e UploadNodeInfo, i int) bool { return e.IsSameLocation })
	if len(sameLocationNodes) > 0 {
		return sameLocationNodes[rand.Intn(len(sameLocationNodes))]
	}

	return nodes[rand.Intn(len(nodes))]
}

func uploadToNode(file io.ReadCloser, nodeIP string, uploadConfig UploadConfig) (string, error) {
	// 建立grpc连接，发送请求
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, uploadConfig.GRPCPort)
	grpcCon, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}
	defer grpcCon.Close()

	client := agentcaller.NewFileTransportClient(grpcCon)
	upload, err := mygrpc.SendFileAsStream(client)
	if err != nil {
		return "", fmt.Errorf("request to send file failed, err: %w", err)
	}

	// 发送文件数据
	_, err = io.Copy(upload, file)
	if err != nil {
		// 发生错误则关闭连接
		upload.Abort(io.ErrClosedPipe)
		return "", fmt.Errorf("copy file date to upload stream failed, err: %w", err)
	}

	// 发送EOF消息，并获得FileHash
	fileHash, err := upload.Finish()
	if err != nil {
		upload.Abort(io.ErrClosedPipe)
		return "", fmt.Errorf("send EOF failed, err: %w", err)
	}

	return fileHash, nil
}

func uploadToLocalIPFS(ipfs *ipfs.IPFS, file io.ReadCloser, nodeID int64, shouldPin bool, uploadConfig UploadConfig) (string, error) {
	// 从本地IPFS上传文件
	writer, err := ipfs.CreateFile()
	if err != nil {
		return "", fmt.Errorf("create IPFS file failed, err: %w", err)
	}

	_, err = io.Copy(writer, file)
	if err != nil {
		return "", fmt.Errorf("copy file data to IPFS failed, err: %w", err)
	}

	fileHash, err := writer.Finish()
	if err != nil {
		return "", fmt.Errorf("finish writing IPFS failed, err: %w", err)
	}

	if !shouldPin {
		return fileHash, nil
	}

	// 然后让最近节点pin本地上传的文件
	agentClient, err := agtmq.NewClient(nodeID, uploadConfig.MQ)
	if err != nil {
		return "", fmt.Errorf("create agent client to %d failed, err: %w", nodeID, err)
	}
	defer agentClient.Close()

	pinObjResp, err := agentClient.StartPinningObject(agtmq.NewStartPinningObject(fileHash))
	if err != nil {
		return "", fmt.Errorf("start pinning object: %w", err)
	}

	for {
		waitResp, err := agentClient.WaitPinningObject(agtmq.NewWaitPinningObject(pinObjResp.TaskID, int64(time.Second)*5))
		if err != nil {
			return "", fmt.Errorf("waitting pinning object: %w", err)
		}

		if waitResp.IsComplete {
			if waitResp.Error != "" {
				return "", fmt.Errorf("agent pinning object: %s", waitResp.Error)
			}

			break
		}
	}

	return fileHash, nil
}
