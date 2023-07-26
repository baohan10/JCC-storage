package services

import (
	"time"

	"gitlink.org.cn/cloudream/client/internal/task"
)

type StorageService struct {
	*Service
}

func (svc *Service) StorageSvc() *StorageService {
	return &StorageService{Service: svc}
}

func (svc *StorageService) StartMovingObjectToStorage(userID int64, objectID int64, storageID int64) (string, error) {
	tsk := svc.taskMgr.StartNew(task.NewMoveObjectToStorage(userID, objectID, storageID))
	return tsk.ID(), nil
}

func (svc *StorageService) WaitMovingObjectToStorage(taskID string, waitTimeout time.Duration) (bool, error) {
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
