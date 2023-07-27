package services

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/client/internal/task"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

type StorageService struct {
	*Service
}

func (svc *Service) StorageSvc() *StorageService {
	return &StorageService{Service: svc}
}

func (svc *StorageService) StartStorageMoveObject(userID int64, objectID int64, storageID int64) (string, error) {
	tsk := svc.taskMgr.StartNew(task.NewMoveObjectToStorage(userID, objectID, storageID))
	return tsk.ID(), nil
}

func (svc *StorageService) WaitStorageMoveObjectToStorage(taskID string, waitTimeout time.Duration) (bool, error) {
	tsk := svc.taskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		return true, tsk.Error()
	}

	return false, nil
}

func (svc *StorageService) DeleteStorageObject(userID int64, objectID int64, storageID int64) error {
	// TODO
	panic("not implement yet")
}

// 请求节点启动从Storage中上传文件的任务。会返回节点ID和任务ID
func (svc *StorageService) StartStorageUploadRepObject(userID int64, storageID int64, filePath string, bucketID int64, objectName string, repCount int) (int64, string, error) {
	stgResp, err := svc.coordinator.GetStorageInfo(coormsg.NewGetStorageInfo(userID, storageID))
	if err != nil {
		return 0, "", fmt.Errorf("getting storage info: %w", err)
	}

	agentCli, err := agtcli.NewClient(stgResp.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return 0, "", fmt.Errorf("new agent client: %w", err)
	}
	defer agentCli.Close()

	startResp, err := agentCli.StartStorageUploadRepObject(agtmsg.NewStartStorageUploadRepObject(userID, filePath, bucketID, objectName, repCount, stgResp.Directory))
	if err != nil {
		return 0, "", fmt.Errorf("start storage upload rep object: %w", err)
	}

	return stgResp.NodeID, startResp.TaskID, nil
}

func (svc *StorageService) WaitStorageUploadRepObject(nodeID int64, taskID string, waitTimeout time.Duration) (bool, int64, string, error) {
	agentCli, err := agtcli.NewClient(nodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		// TODO 失败是否要当做任务已经结束？
		return true, 0, "", fmt.Errorf("new agent client: %w", err)
	}
	defer agentCli.Close()

	waitResp, err := agentCli.WaitStorageUploadRepObject(agtmsg.NewWaitStorageUploadRepObject(taskID, waitTimeout.Milliseconds()))
	if err != nil {
		// TODO 请求失败是否要当做任务已经结束？
		return true, 0, "", fmt.Errorf("wait storage upload rep object: %w", err)
	}

	if !waitResp.IsComplete {
		return false, 0, "", nil
	}

	if waitResp.Error != "" {
		return true, 0, "", fmt.Errorf("%s", waitResp.Error)
	}

	return true, waitResp.ObjectID, waitResp.FileHash, nil
}
