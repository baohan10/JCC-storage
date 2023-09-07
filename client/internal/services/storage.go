package services

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/storage/client/internal/task"
	"gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type StorageService struct {
	*Service
}

func (svc *Service) StorageSvc() *StorageService {
	return &StorageService{Service: svc}
}

func (svc *StorageService) StartStorageLoadPackage(userID int64, packageID int64, storageID int64) (string, error) {
	tsk := svc.TaskMgr.StartNew(task.NewStorageLoadPackage(userID, packageID, storageID))
	return tsk.ID(), nil
}

func (svc *StorageService) WaitStorageLoadPackage(taskID string, waitTimeout time.Duration) (bool, error) {
	tsk := svc.TaskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		return true, tsk.Error()
	}
	return false, nil
}

func (svc *StorageService) DeleteStoragePackage(userID int64, packageID int64, storageID int64) error {
	// TODO
	panic("not implement yet")
}

// 请求节点启动从Storage中上传文件的任务。会返回节点ID和任务ID
func (svc *StorageService) StartStorageCreatePackage(userID int64, bucketID int64, name string, storageID int64, path string, redundancy models.TypedRedundancyInfo, nodeAffinity *int64) (int64, string, error) {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return 0, "", fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

	stgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(userID, storageID))
	if err != nil {
		return 0, "", fmt.Errorf("getting storage info: %w", err)
	}

	agentCli, err := globals.AgentMQPool.Acquire(stgResp.NodeID)
	if err != nil {
		return 0, "", fmt.Errorf("new agent client: %w", err)
	}
	defer agentCli.Close()

	startResp, err := agentCli.StartStorageCreatePackage(agtmq.NewStartStorageCreatePackage(userID, bucketID, name, storageID, path, redundancy, nodeAffinity))
	if err != nil {
		return 0, "", fmt.Errorf("start storage upload package: %w", err)
	}

	return stgResp.NodeID, startResp.TaskID, nil
}

func (svc *StorageService) WaitStorageCreatePackage(nodeID int64, taskID string, waitTimeout time.Duration) (bool, int64, error) {
	agentCli, err := globals.AgentMQPool.Acquire(nodeID)
	if err != nil {
		// TODO 失败是否要当做任务已经结束？
		return true, 0, fmt.Errorf("new agent client: %w", err)
	}
	defer agentCli.Close()

	waitResp, err := agentCli.WaitStorageCreatePackage(agtmq.NewWaitStorageCreatePackage(taskID, waitTimeout.Milliseconds()))
	if err != nil {
		// TODO 请求失败是否要当做任务已经结束？
		return true, 0, fmt.Errorf("wait storage upload package: %w", err)
	}

	if !waitResp.IsComplete {
		return false, 0, nil
	}

	if waitResp.Error != "" {
		return true, 0, fmt.Errorf("%s", waitResp.Error)
	}

	return true, waitResp.PackageID, nil
}
