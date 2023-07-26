package services

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/client/internal/task"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	mygrpc "gitlink.org.cn/cloudream/common/utils/grpc"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	serder "gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/db/model"
	agentcaller "gitlink.org.cn/cloudream/proto"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	lo "github.com/samber/lo"
)

type ObjectService struct {
	*Service
}

func (svc *Service) ObjectSvc() *ObjectService {
	return &ObjectService{Service: svc}
}

func (svc *ObjectService) GetObject(userID int, objectID int) (model.Object, error) {
	// TODO
	panic("not implement yet")
}

func (svc *ObjectService) DownloadObject(userID int, objectID int) (io.ReadCloser, error) {
	mutex, err := reqbuilder.NewBuilder().
		// 用于判断用户是否有对象权限
		Metadata().UserBucket().ReadAny().
		// 用于查询可用的下载节点
		Node().ReadAny().
		// 用于读取文件信息
		Object().ReadOne(objectID).
		// 用于查询Rep配置
		ObjectRep().ReadOne(objectID).
		// 用于查询Block配置
		ObjectBlock().ReadAny().
		// 用于查询包含了副本的节点
		Cache().ReadAny().
		MutexLock(svc.distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}

	preDownloadResp, err := svc.coordinator.PreDownloadObject(coormsg.NewPreDownloadObject(objectID, userID, config.Cfg().ExternalIP))
	if err != nil {
		mutex.Unlock()
		return nil, fmt.Errorf("pre download object: %w", err)
	}

	switch preDownloadResp.Redundancy {
	case consts.REDUNDANCY_REP:
		var repInfo ramsg.RespObjectRepInfo
		err := serder.MapToObject(preDownloadResp.RedundancyData.(map[string]any), &repInfo)
		if err != nil {
			mutex.Unlock()
			return nil, fmt.Errorf("redundancy data to rep info failed, err: %w", err)
		}

		if len(repInfo.Nodes) == 0 {
			mutex.Unlock()
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

		reader, err := svc.downloadRepObject(entry.ID, nodeIP, repInfo.FileHash)
		if err != nil {
			mutex.Unlock()
			return nil, fmt.Errorf("rep read failed, err: %w", err)
		}

		return myio.AfterReadClosed(reader, func(closer io.ReadCloser) {
			// TODO 可以考虑在打开了读取流之后就解锁，而不是要等外部读取完毕
			mutex.Unlock()
		}), nil

		//case consts.REDUNDANCY_EC:
		// TODO EC部分的代码要考虑重构
		//	ecRead(readResp.FileSize, readResp.NodeIPs, readResp.Hashes, readResp.BlockIDs, *readResp.ECName)
	}

	mutex.Unlock()
	return nil, fmt.Errorf("unsupported redundancy type: %s", preDownloadResp.Redundancy)
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

func (svc *ObjectService) downloadRepObject(nodeID int, nodeIP string, fileHash string) (io.ReadCloser, error) {
	if svc.ipfs != nil {
		log.Infof("try to use local IPFS to download file")

		reader, err := svc.downloadFromLocalIPFS(fileHash)
		if err == nil {
			return reader, nil
		}

		log.Warnf("download from local IPFS failed, so try to download from node %s, err: %s", nodeIP, err.Error())
	}

	return svc.downloadFromNode(nodeID, nodeIP, fileHash)
}

func (svc *ObjectService) downloadFromNode(nodeID int, nodeIP string, fileHash string) (io.ReadCloser, error) {
	// 二次获取锁
	mutex, err := reqbuilder.NewBuilder().
		// 用于从IPFS下载文件
		IPFS().ReadOneRep(nodeID, fileHash).
		MutexLock(svc.distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}

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

	reader = myio.AfterReadClosed(reader, func(io.ReadCloser) {
		conn.Close()
		mutex.Unlock()
	})
	return reader, nil
}

func (svc *ObjectService) downloadFromLocalIPFS(fileHash string) (io.ReadCloser, error) {
	// TODO 这里也可以改成Task
	reader, err := svc.ipfs.OpenRead(fileHash)
	if err != nil {
		return nil, fmt.Errorf("read ipfs file failed, err: %w", err)
	}

	return reader, nil
}

func (svc *ObjectService) StartUploadingRepObjects(userID int, bucketID int, uploadObjects []task.UploadObject, repCount int) (string, error) {
	tsk := svc.taskMgr.StartNew(task.NewUploadRepObject(userID, bucketID, uploadObjects, repCount))
	return tsk.ID(), nil
}

func (svc *ObjectService) WaitUploadingRepObjects(taskID string, waitTimeout time.Duration) (bool, task.UploadObjectResult, error) {
	tsk := svc.taskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		uploadObjectResult := task.UploadObjectResult{
			UploadObjects:    tsk.Body().(*task.UploadRepObject).UploadObjects,
			UploadRepResults: tsk.Body().(*task.UploadRepObject).UploadRepResults,
			IsUploading:      tsk.Body().(*task.UploadRepObject).IsUploading,
		}

		return true, uploadObjectResult, tsk.Error()
	}
	return false, task.UploadObjectResult{}, nil
}

func (svc *ObjectService) UploadECObject(userID int, file io.ReadCloser, fileSize int64, ecName string) error {
	// TODO
	panic("not implement yet")
}

func (svc *ObjectService) StartUpdatingRepObject(userID int, objectID int, file io.ReadCloser, fileSize int64) (string, error) {
	tsk := svc.taskMgr.StartNew(task.NewUpdateRepObject(userID, objectID, file, fileSize))
	return tsk.ID(), nil
}

func (svc *ObjectService) WaitUpdatingRepObject(taskID string, waitTimeout time.Duration) (bool, error) {
	tsk := svc.taskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		return true, tsk.Error()
	}

	return false, nil
}

func (svc *ObjectService) DeleteObject(userID int, objectID int) error {
	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于判断用户是否有对象的权限
		UserBucket().ReadAny().
		// 用于读取、修改对象信息
		Object().WriteOne(objectID).
		// 用于删除Rep配置
		ObjectRep().WriteOne(objectID).
		// 用于删除Block配置
		ObjectBlock().WriteAny().
		// 用于修改Move此Object的记录的状态
		StorageObject().WriteAny().
		MutexLock(svc.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	_, err = svc.coordinator.DeleteObject(coormsg.NewDeleteObject(userID, objectID))
	if err != nil {
		return fmt.Errorf("deleting object: %w", err)
	}

	return nil
}
