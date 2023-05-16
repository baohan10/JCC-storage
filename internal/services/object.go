package services

import (
	"fmt"
	"io"
	"math/rand"

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/db/model"
	agentcaller "gitlink.org.cn/cloudream/proto"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils/consts"
	mygrpc "gitlink.org.cn/cloudream/utils/grpc"
	myio "gitlink.org.cn/cloudream/utils/io"
	log "gitlink.org.cn/cloudream/utils/logger"
	serder "gitlink.org.cn/cloudream/utils/serder"
	mysort "gitlink.org.cn/cloudream/utils/sort"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	lo "github.com/samber/lo"
)

type ObjectService struct {
	*Service
}

func ObjectSvc(svc *Service) *ObjectService {
	return &ObjectService{Service: svc}
}

func (svc *ObjectService) GetObject(userID int, objectID int) (model.Object, error) {
	// TODO
	panic("not implement yet")
}

func (svc *ObjectService) DownloadObject(userID int, objectID int) (io.ReadCloser, error) {
	preDownloadResp, err := svc.coordinator.PreDownloadObject(coormsg.NewPreDownloadObjectBody(objectID, userID, config.Cfg().ExternalIP))
	if err != nil {
		return nil, fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if preDownloadResp.IsFailed() {
		return nil, fmt.Errorf("coordinator operation failed, code: %s, message: %s", preDownloadResp.ErrorCode, preDownloadResp.ErrorMessage)
	}

	switch preDownloadResp.Body.Redundancy {
	case consts.REDUNDANCY_REP:
		var repInfo ramsg.RespObjectRepInfo
		err := serder.MapToObject(preDownloadResp.Body.RedundancyData.(map[string]any), &repInfo)
		if err != nil {
			return nil, fmt.Errorf("redundancy data to rep info failed, err: %w", err)
		}

		if len(repInfo.Nodes) == 0 {
			return nil, fmt.Errorf("no node has this file")
		}

		// 选择下载节点
		entry := svc.chooseDownloadNode(repInfo.Nodes)

		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		nodeIP := entry.ExternalIP
		if entry.IsSameLocation {
			nodeIP = entry.LocalIP

			log.Infof("client and node %d are at the same location, use local ip\n", entry.ID)
		}

		reader, err := svc.downloadRepObject(nodeIP, repInfo.FileHash)
		if err != nil {
			return nil, fmt.Errorf("rep read failed, err: %w", err)
		}

		return reader, nil

		//case consts.REDUNDANCY_EC:
		// TODO EC部分的代码要考虑重构
		//	ecRead(readResp.FileSize, readResp.NodeIPs, readResp.Hashes, readResp.BlockIDs, *readResp.ECName)
	}

	return nil, fmt.Errorf("unsupported redundancy type: %s", preDownloadResp.Body.Redundancy)
}

// chooseDownloadNode 选择一个下载节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (svc *ObjectService) chooseDownloadNode(entries []ramsg.RespNode) ramsg.RespNode {
	sameLocationEntries := lo.Filter(entries, func(e ramsg.RespNode, i int) bool { return e.IsSameLocation })
	if len(sameLocationEntries) > 0 {
		return sameLocationEntries[rand.Intn(len(sameLocationEntries))]
	}

	return entries[rand.Intn(len(entries))]
}

func (svc *ObjectService) downloadRepObject(nodeIP string, fileHash string) (io.ReadCloser, error) {
	if svc.ipfs != nil {
		log.Infof("try to use local IPFS to download file")

		reader, err := svc.downloadFromLocalIPFS(fileHash)
		if err == nil {
			return reader, nil
		}

		log.Warnf("download from local IPFS failed, so try to download from node %s, err: %s", nodeIP, err.Error())
	}

	return svc.downloadFromNode(nodeIP, fileHash)
}

func (svc *ObjectService) downloadFromNode(nodeIP string, fileHash string) (io.ReadCloser, error) {
	// 连接grpc
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
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

	reader = myio.AfterReadClosed(reader, func(io.ReadCloser) { conn.Close() })
	return reader, nil
}

func (svc *ObjectService) downloadFromLocalIPFS(fileHash string) (io.ReadCloser, error) {
	reader, err := svc.ipfs.OpenRead(fileHash)
	if err != nil {
		return nil, fmt.Errorf("read ipfs file failed, err: %w", err)
	}

	return reader, nil
}

func (svc *ObjectService) UploadRepObject(userID int, bucketID int, objectName string, file io.ReadCloser, fileSize int64, repCount int) error {

	//发送写请求，请求Coor分配写入节点Ip
	repWriteResp, err := svc.coordinator.PreUploadRepObject(coormsg.NewPreUploadRepObjectBody(bucketID, objectName, fileSize, userID, config.Cfg().ExternalIP))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if repWriteResp.IsFailed() {
		return fmt.Errorf("coordinator RepWrite failed, code: %s, message: %s", repWriteResp.ErrorCode, repWriteResp.ErrorMessage)
	}

	if len(repWriteResp.Body.Nodes) == 0 {
		return fmt.Errorf("no node to upload file")
	}

	uploadNode := svc.chooseUploadNode(repWriteResp.Body.Nodes)

	var fileHash string
	uploadedNodeIDs := []int{}
	uploadToNode := true
	// 本地有IPFS，则直接从本地IPFS上传
	if svc.ipfs != nil {
		log.Infof("try to use local IPFS to upload file")

		fileHash, err = svc.uploadToLocalIPFS(file, uploadNode.ID)
		if err != nil {
			log.Warnf("upload to local IPFS failed, so try to upload to node %d, err: %s", uploadNode.ID, err.Error())
		} else {
			uploadToNode = false
		}
	}

	// 否则发送到agent上传
	if uploadToNode {
		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		nodeIP := uploadNode.ExternalIP
		if uploadNode.IsSameLocation {
			nodeIP = uploadNode.LocalIP

			log.Infof("client and node %d are at the same location, use local ip\n", uploadNode.ID)
		}

		fileHash, err = svc.uploadToNode(file, nodeIP)
		if err != nil {
			return fmt.Errorf("upload to node %s failed, err: %w", nodeIP, err)
		}
		uploadedNodeIDs = append(uploadedNodeIDs, uploadNode.ID)
	}

	// 记录写入的文件的Hash
	createObjectResp, err := svc.coordinator.CreateRepObject(coormsg.NewCreateRepObjectBody(bucketID, objectName, fileSize, repCount, userID, uploadedNodeIDs, fileHash))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if createObjectResp.IsFailed() {
		return fmt.Errorf("coordinator CreateRepObject failed, code: %s, message: %s", createObjectResp.ErrorCode, createObjectResp.ErrorMessage)
	}

	return nil
}

func (svc *ObjectService) uploadToNode(file io.ReadCloser, nodeIP string) (string, error) {
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

func (svc *ObjectService) uploadToLocalIPFS(file io.ReadCloser, nodeID int) (string, error) {
	// 从本地IPFS上传文件
	writer, err := svc.ipfs.CreateFile()
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
	agentClient, err := agtcli.NewAgentClient(nodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return "", fmt.Errorf("create agent client to %d failed, err: %w", nodeID, err)
	}
	defer agentClient.Close()

	pinObjResp, err := agentClient.PinObject(agtmsg.NewPinObjectBody(fileHash))
	if err != nil {
		return "", fmt.Errorf("request to agent %d failed, err: %w", nodeID, err)
	}
	if pinObjResp.IsFailed() {
		return "", fmt.Errorf("agent %d PinObject failed, code: %s, message: %s", nodeID, pinObjResp.ErrorCode, pinObjResp.ErrorMessage)
	}

	return fileHash, nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (svc *ObjectService) chooseUploadNode(nodes []ramsg.RespNode) ramsg.RespNode {
	sameLocationNodes := lo.Filter(nodes, func(e ramsg.RespNode, i int) bool { return e.IsSameLocation })
	if len(sameLocationNodes) > 0 {
		return sameLocationNodes[rand.Intn(len(sameLocationNodes))]
	}

	return nodes[rand.Intn(len(nodes))]
}

func (svc *ObjectService) UploadECObject(userID int, file io.ReadCloser, fileSize int64, ecName string) error {
	// TODO
	panic("not implement yet")
}

func (svc *ObjectService) UpdateRepObject(userID int, objectID int, file io.ReadCloser, fileSize int64) error {
	preResp, err := svc.coordinator.PreUpdateRepObject(coormsg.NewPreUpdateRepObjectBody(
		objectID,
		fileSize,
		userID,
		config.Cfg().ExternalIP,
	))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if preResp.IsFailed() {
		return fmt.Errorf("coordinator PreUpdateRepObject failed, code: %s, message: %s", preResp.ErrorCode, preResp.ErrorMessage)
	}

	if len(preResp.Body.Nodes) == 0 {
		return fmt.Errorf("no node to upload file")
	}

	// 上传文件的方式优先级：
	// 1. 本地IPFS
	// 2. 包含了旧文件，且与客户端在同地域的节点
	// 3. 不在同地域，但包含了旧文件的节点
	// 4. 同地域节点

	uploadNode := svc.chooseUpdateRepObjectNode(preResp.Body.Nodes)

	var fileHash string
	uploadedNodeIDs := []int{}
	uploadToNode := true
	// 本地有IPFS，则直接从本地IPFS上传
	if svc.ipfs != nil {
		log.Infof("try to use local IPFS to upload file")

		fileHash, err = svc.uploadToLocalIPFS(file, uploadNode.ID)
		if err != nil {
			log.Warnf("upload to local IPFS failed, so try to upload to node %d, err: %s", uploadNode.ID, err.Error())
		} else {
			uploadToNode = false
		}
	}

	// 否则发送到agent上传
	if uploadToNode {
		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		nodeIP := uploadNode.ExternalIP
		if uploadNode.IsSameLocation {
			nodeIP = uploadNode.LocalIP

			log.Infof("client and node %d are at the same location, use local ip\n", uploadNode.ID)
		}

		fileHash, err = svc.uploadToNode(file, nodeIP)
		if err != nil {
			return fmt.Errorf("upload to node %s failed, err: %w", nodeIP, err)
		}
		uploadedNodeIDs = append(uploadedNodeIDs, uploadNode.ID)
	}

	// 更新Object
	updateResp, err := svc.coordinator.UpdateRepObject(coormsg.NewUpdateRepObjectBody(objectID, fileHash, fileSize, uploadedNodeIDs, userID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if updateResp.IsFailed() {
		return fmt.Errorf("coordinator UpdateRepObject failed, code: %s, message: %s", updateResp.ErrorCode, updateResp.ErrorMessage)
	}

	return nil
}

func (svc *ObjectService) chooseUpdateRepObjectNode(nodes []coormsg.PreUpdateRepObjectRespNode) coormsg.PreUpdateRepObjectRespNode {
	mysort.Sort(nodes, func(left, right coormsg.PreUpdateRepObjectRespNode) int {
		v := -mysort.CmpBool(left.HasOldObject, right.HasOldObject)
		if v != 0 {
			return v
		}

		return -mysort.CmpBool(left.IsSameLocation, right.IsSameLocation)
	})

	return nodes[0]
}

func (svc *ObjectService) DeleteObject(userID int, objectID int) error {
	resp, err := svc.coordinator.DeleteObject(coormsg.NewDeleteObjectBody(userID, objectID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if resp.IsFailed() {
		return fmt.Errorf("create bucket objects failed, code: %s, message: %s", resp.ErrorCode, resp.ErrorMessage)
	}

	return nil
}
