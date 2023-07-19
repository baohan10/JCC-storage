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

func (svc *StorageService) StartMovingObjectToStorage(userID int, objectID int, storageID int) (string, error) {
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

func (svc *StorageService) DeleteStorageObject(userID int, objectID int, storageID int) error {
	// TODO
	panic("not implement yet")
}
