package task

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/common/utils"
	mygrpc "gitlink.org.cn/cloudream/common/utils/grpc"
	"gitlink.org.cn/cloudream/common/utils/ipfs"

	agentcaller "gitlink.org.cn/cloudream/proto"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// UploadObjects和UploadRepResults为一一对应关系
type UploadRepObjects struct {
	userID      int64
	bucketID    int64
	repCount    int
	Objects     []UploadObject
	Results     []UploadSingleRepObjectResult
	IsUploading bool
}

type UploadRepObjectsResult struct {
	Objects     []UploadObject
	Results     []UploadSingleRepObjectResult
	IsUploading bool
}

type UploadObject struct {
	ObjectName string
	File       io.ReadCloser
	FileSize   int64
}

type UploadSingleRepObjectResult struct {
	Error    error
	FileHash string
	ObjectID int64
}

func NewUploadRepObjects(userID int64, bucketID int64, uploadObjects []UploadObject, repCount int) *UploadRepObjects {
	return &UploadRepObjects{
		userID:   userID,
		bucketID: bucketID,
		Objects:  uploadObjects,
		repCount: repCount,
	}
}

func (t *UploadRepObjects) Execute(ctx TaskContext, complete CompleteFn) {
	log := logger.WithType[UploadRepObjects]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
	for _, obj := range t.Objects {
		obj.File.Close()
	}
}

func (t *UploadRepObjects) do(ctx TaskContext) error {

	reqBlder := reqbuilder.NewBuilder()
	for _, uploadObject := range t.Objects {
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
	hasFailure := true
	for i := 0; i < len(t.Objects); i++ {
		repWriteResp, err := t.preUploadSingleObject(ctx, t.Objects[i])
		if err != nil {
			hasFailure = false
			t.Results = append(t.Results,
				UploadSingleRepObjectResult{
					Error:    err,
					FileHash: "",
					ObjectID: 0,
				})
			continue
		}
		t.Results = append(t.Results, UploadSingleRepObjectResult{})
		repWriteResps = append(repWriteResps, repWriteResp)
	}

	// 不满足上传条件，返回各文件检查结果
	if !hasFailure {
		return nil
	}

	//上传文件夹
	t.IsUploading = true
	for i := 0; i < len(repWriteResps); i++ {
		objectID, fileHash, err := t.uploadSingleObject(ctx, t.Objects[i], repWriteResps[i])
		// 记录文件上传结果
		t.Results[i] = UploadSingleRepObjectResult{
			Error:    err,
			FileHash: fileHash,
			ObjectID: objectID,
		}
	}
	return nil
}

// 检查单个文件是否能够上传
func (t *UploadRepObjects) preUploadSingleObject(ctx TaskContext, uploadObject UploadObject) (*coormsg.PreUploadResp, error) {
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
func (t *UploadRepObjects) uploadSingleObject(ctx TaskContext, uploadObject UploadObject, preResp *coormsg.PreUploadResp) (int64, string, error) {

	var fileHash string
	uploadedNodeIDs := []int64{}
	willUploadToNode := true

	// 因为本地的IPFS属于调度系统的一部分，所以需要加锁
	mutex, err := reqbuilder.NewBuilder().
		IPFS().CreateAnyRep(config.Cfg().ID).
		MutexLock(ctx.DistLock)
	if err != nil {
		return 0, "", fmt.Errorf("acquiring locks: %w", err)
	}

	fileHash, err = uploadToLocalIPFS(ctx.IPFS, uploadObject.File)
	if err != nil {
		// 上传失败，则立刻解锁
		mutex.Unlock()

		logger.Warnf("uploading to local IPFS: %s, will select a node to upload", err.Error())

	} else {
		willUploadToNode = false
		uploadedNodeIDs = append(uploadedNodeIDs, config.Cfg().ID)

		// 上传成功，则等到所有操作结束后才能解锁
		defer mutex.Unlock()
	}

	// 本地IPFS失败，则发送到agent上传
	if willUploadToNode {
		// 本地IPFS已经失败，所以不要再选择当前节点了
		uploadNode := t.chooseUploadNode(lo.Reject(preResp.Nodes, func(item ramsg.RespNode, index int) bool { return item.ID == config.Cfg().ID }))

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

	dirName := utils.GetDirectoryName(uploadObject.ObjectName)

	// 记录写入的文件的Hash
	createResp, err := ctx.Coordinator.CreateRepObject(coormsg.NewCreateRepObject(t.bucketID, uploadObject.ObjectName, uploadObject.FileSize, t.repCount, t.userID, uploadedNodeIDs, fileHash, dirName))
	if err != nil {
		return 0, "", fmt.Errorf("creating rep object: %w", err)
	}

	return createResp.ObjectID, fileHash, nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (t *UploadRepObjects) chooseUploadNode(nodes []ramsg.RespNode) ramsg.RespNode {
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

func uploadToLocalIPFS(ipfs *ipfs.IPFS, file io.ReadCloser) (string, error) {
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

	return fileHash, nil
}
