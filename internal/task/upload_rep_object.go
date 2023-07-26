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

// UploadObjects和UploadRepResults为一一对应关系
type UploadRepObject struct {
	userID           int
	bucketID         int
	repCount         int
	UploadObjects    []UploadObject
	UploadRepResults []UploadRepResult
	IsUploading      bool
}

type UploadObjectResult struct {
	UploadObjects    []UploadObject
	UploadRepResults []UploadRepResult
	IsUploading      bool
}

type UploadObject struct {
	ObjectName string
	File       io.ReadCloser
	FileSize   int64
}

type UploadRepResult struct {
	Error          error
	ResultFileHash string
	ObjectID       int64
}

func NewUploadRepObject(userID int, bucketID int, UploadObjects []UploadObject, repCount int) *UploadRepObject {
	return &UploadRepObject{
		userID:        userID,
		bucketID:      bucketID,
		UploadObjects: UploadObjects,
		repCount:      repCount,
	}
}

func (t *UploadRepObject) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *UploadRepObject) do(ctx TaskContext) error {

	reqBlder := reqbuilder.NewBuilder()
	for _, uploadObject := range t.UploadObjects {
		reqBlder.Metadata().
			// 用于防止创建了多个同名对象
			Object().CreateOne(t.bucketID, uploadObject.ObjectName)
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
		MutexLock(ctx.DistLock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	var repWriteResps []*coormsg.PreUploadResp

	//判断是否所有文件都符合上传条件
	flag := true
	for i := 0; i < len(t.UploadObjects); i++ {
		repWriteResp, err := t.preUploadSingleObject(ctx, t.UploadObjects[i])
		if err != nil {
			flag = false
			t.UploadRepResults = append(t.UploadRepResults,
				UploadRepResult{
					Error:          err,
					ResultFileHash: "",
					ObjectID:       0,
				})
			continue
		}
		t.UploadRepResults = append(t.UploadRepResults, UploadRepResult{})
		repWriteResps = append(repWriteResps, repWriteResp)
	}

	// 不满足上传条件，返回各文件检查结果
	if !flag {
		return nil
	}

	//上传文件夹
	t.IsUploading = true
	for i := 0; i < len(repWriteResps); i++ {
		objectID, fileHash, err := t.uploadSingleObject(ctx, t.UploadObjects[i], repWriteResps[i].Nodes)
		// 记录文件上传结果
		t.UploadRepResults[i] = UploadRepResult{
			Error:          err,
			ResultFileHash: fileHash,
			ObjectID:       objectID,
		}
	}
	return nil
}

// 检查单个文件是否能够上传
func (t *UploadRepObject) preUploadSingleObject(ctx TaskContext, uploadObject UploadObject) (*coormsg.PreUploadResp, error) {
	//发送写请求，请求Coor分配写入节点Ip
	repWriteResp, err := ctx.Coordinator.PreUploadRepObject(coormsg.NewPreUploadRepObjectBody(t.bucketID, uploadObject.ObjectName, uploadObject.FileSize, t.userID, config.Cfg().ExternalIP))
	if err != nil {
		return nil, fmt.Errorf("pre upload rep object: %w", err)
	}
	if len(repWriteResp.Nodes) == 0 {
		return nil, fmt.Errorf("no node to upload file")
	}
	return repWriteResp, nil
}

// 上传文件
func (t *UploadRepObject) uploadSingleObject(ctx TaskContext, uploadObject UploadObject, nodes []ramsg.RespNode) (int64, string, error) {

	uploadNode := t.chooseUploadNode(nodes)

	var fileHash string
	uploadedNodeIDs := []int{}
	willUploadToNode := true
	// 本地有IPFS，则直接从本地IPFS上传
	if ctx.IPFS != nil {
		logger.Infof("try to use local IPFS to upload file")

		var err error
		fileHash, err = uploadToLocalIPFS(ctx.IPFS, uploadObject.File, uploadNode.ID)
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
			return 0, "", fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()

		fileHash, err = uploadToNode(uploadObject.File, nodeIP)
		if err != nil {
			return 0, "", fmt.Errorf("upload to node %s failed, err: %w", nodeIP, err)
		}
		uploadedNodeIDs = append(uploadedNodeIDs, uploadNode.ID)
	}

	// 记录写入的文件的Hash
	createResp, err := ctx.Coordinator.CreateRepObject(coormsg.NewCreateRepObject(t.bucketID, uploadObject.ObjectName, uploadObject.FileSize, t.repCount, t.userID, uploadedNodeIDs, fileHash))
	if err != nil {
		return 0, "", fmt.Errorf("creating rep object: %w", err)
	}

	return createResp.ObjectID, fileHash, nil
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
