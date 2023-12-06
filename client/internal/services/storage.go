package services

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/storage/client/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type StorageService struct {
	*Service
}

func (svc *Service) StorageSvc() *StorageService {
	return &StorageService{Service: svc}
}

func (svc *StorageService) StartStorageLoadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, storageID cdssdk.StorageID) (string, error) {
	tsk := svc.TaskMgr.StartNew(task.NewStorageLoadPackage(userID, packageID, storageID))
	return tsk.ID(), nil
}

func (svc *StorageService) WaitStorageLoadPackage(taskID string, waitTimeout time.Duration) (bool, string, error) {
	tsk := svc.TaskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		loadTsk := tsk.Body().(*task.StorageLoadPackage)
		return true, loadTsk.ResultFullPath, tsk.Error()
	}
	return false, "", nil
}

func (svc *StorageService) DeleteStoragePackage(userID int64, packageID int64, storageID int64) error {
	// TODO
	panic("not implement yet")
}

// 请求节点启动从Storage中上传文件的任务。会返回节点ID和任务ID
func (svc *StorageService) StartStorageCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, storageID cdssdk.StorageID, path string, nodeAffinity *cdssdk.NodeID) (cdssdk.NodeID, string, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return 0, "", fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	stgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(userID, storageID))
	if err != nil {
		return 0, "", fmt.Errorf("getting storage info: %w", err)
	}

	agentCli, err := stgglb.AgentMQPool.Acquire(stgResp.NodeID)
	if err != nil {
		return 0, "", fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	startResp, err := agentCli.StartStorageCreatePackage(agtmq.NewStartStorageCreatePackage(userID, bucketID, name, storageID, path, nodeAffinity))
	if err != nil {
		return 0, "", fmt.Errorf("start storage upload package: %w", err)
	}

	return stgResp.NodeID, startResp.TaskID, nil
}

func (svc *StorageService) WaitStorageCreatePackage(nodeID cdssdk.NodeID, taskID string, waitTimeout time.Duration) (bool, cdssdk.PackageID, error) {
	agentCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		// TODO 失败是否要当做任务已经结束？
		return true, 0, fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

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

func (svc *StorageService) GetInfo(userID cdssdk.UserID, storageID cdssdk.StorageID) (*model.Storage, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(userID, storageID))
	if err != nil {
		return nil, fmt.Errorf("request to coordinator: %w", err)
	}

	return &getResp.Storage, nil
}
