package task

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	mygrpc "gitlink.org.cn/cloudream/common/utils/grpc"
	"gitlink.org.cn/cloudream/common/utils/ipfs"

	agentcaller "gitlink.org.cn/cloudream/proto"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type UploadRepObject struct {
	userID     int
	bucketID   int
	objectName string
	file       io.ReadCloser
	fileSize   int64
	repCount   int

	ResultFileHash string
}

func NewUploadRepObject(userID int, bucketID int, objectName string, file io.ReadCloser, fileSize int64, repCount int) *UploadRepObject {
	return &UploadRepObject{
		userID:     userID,
		bucketID:   bucketID,
		objectName: objectName,
		file:       file,
		fileSize:   fileSize,
		repCount:   repCount,
	}
}

func (t *UploadRepObject) Execute(ctx TaskContext, complete CompleteFn) {
	fileHash, err := t.do(ctx)
	t.ResultFileHash = fileHash
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *UploadRepObject) do(ctx TaskContext) (string, error) {
	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于判断用户是否有桶的权限
		UserBucket().ReadOne(t.userID, t.bucketID).
		// 用于防止创建了多个同名对象
		Object().CreateOne(t.bucketID, t.objectName).
		// 用于查询可用的上传节点
		Node().ReadAny().
		// 用于设置Rep配置
		ObjectRep().CreateAny().
		// 用于创建Cache记录
		Cache().CreateAny().
		MutexLock(ctx.DistLock)
	if err != nil {
		return "", fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	//发送写请求，请求Coor分配写入节点Ip
	repWriteResp, err := ctx.Coordinator.PreUploadRepObject(coormsg.NewPreUploadRepObjectBody(t.bucketID, t.objectName, t.fileSize, t.userID, config.Cfg().ExternalIP))
	if err != nil {
		return "", fmt.Errorf("pre upload rep object: %w", err)
	}
	if len(repWriteResp.Nodes) == 0 {
		return "", fmt.Errorf("no node to upload file")
	}

	uploadNode := t.chooseUploadNode(repWriteResp.Nodes)

	var fileHash string
	uploadedNodeIDs := []int{}
	willUploadToNode := true
	// 本地有IPFS，则直接从本地IPFS上传
	if ctx.IPFS != nil {
		logger.Infof("try to use local IPFS to upload file")

		fileHash, err = uploadToLocalIPFS(ctx.IPFS, t.file, uploadNode.ID)
		if err != nil {
			logger.Warnf("upload to local IPFS failed, so try to upload to node %d, err: %s", uploadNode.ID, err.Error())
		} else {
			willUploadToNode = false
		}
	}

	// 否则发送到agent上传
	if willUploadToNode {
		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		nodeIP := uploadNode.ExternalIP
		if uploadNode.IsSameLocation {
			nodeIP = uploadNode.LocalIP

			logger.Infof("client and node %d are at the same location, use local ip\n", uploadNode.ID)
		}

		mutex, err := reqbuilder.NewBuilder().
			// 防止上传的副本被清除
			IPFS().CreateAnyRep(uploadNode.ID).
			MutexLock(ctx.DistLock)
		if err != nil {
			return "", fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()

		fileHash, err = uploadToNode(t.file, nodeIP)
		if err != nil {
			return "", fmt.Errorf("upload to node %s failed, err: %w", nodeIP, err)
		}
		uploadedNodeIDs = append(uploadedNodeIDs, uploadNode.ID)
	}

	// 记录写入的文件的Hash
	_, err = ctx.Coordinator.CreateRepObject(coormsg.NewCreateRepObject(t.bucketID, t.objectName, t.fileSize, t.repCount, t.userID, uploadedNodeIDs, fileHash))
	if err != nil {
		return "", fmt.Errorf("creating rep object: %w", err)
	}

	return fileHash, nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (t *UploadRepObject) chooseUploadNode(nodes []ramsg.RespNode) ramsg.RespNode {
	sameLocationNodes := lo.Filter(nodes, func(e ramsg.RespNode, i int) bool { return e.IsSameLocation })
	if len(sameLocationNodes) > 0 {
		return sameLocationNodes[rand.Intn(len(sameLocationNodes))]
	}

	return nodes[rand.Intn(len(nodes))]
}

func uploadToNode(file io.ReadCloser, nodeIP string) (string, error) {
	// 建立grpc连接，发送请求
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
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

func uploadToLocalIPFS(ipfs *ipfs.IPFS, file io.ReadCloser, nodeID int) (string, error) {
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

	// 然后让最近节点pin本地上传的文件
	agentClient, err := agtcli.NewClient(nodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return "", fmt.Errorf("create agent client to %d failed, err: %w", nodeID, err)
	}
	defer agentClient.Close()

	pinObjResp, err := agentClient.StartPinningObject(agtmsg.NewStartPinningObject(fileHash))
	if err != nil {
		return "", fmt.Errorf("start pinning object: %w", err)
	}

	for {
		waitResp, err := agentClient.WaitPinningObject(agtmsg.NewWaitPinningObject(pinObjResp.TaskID, int64(time.Second)*5))
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
