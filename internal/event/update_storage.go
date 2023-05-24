package event

import (
	tskcst "gitlink.org.cn/cloudream/common/consts/event"
	"gitlink.org.cn/cloudream/common/utils/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
)

type UpdateStorageEntry struct {
	ObjectID  int
	UserID    int
	Operation string
}

func NewUpdateStorageEntry(objectID int, userID int, op string) UpdateStorageEntry {
	return UpdateStorageEntry{
		ObjectID:  objectID,
		UserID:    userID,
		Operation: op,
	}
}

type UpdateStorage struct {
	StorageID       int
	DirectoryStatus string
	Entries         []UpdateStorageEntry
}

func NewUpdateStorage(dirStatus string, entries []UpdateStorageEntry) UpdateStorage {
	return UpdateStorage{
		DirectoryStatus: dirStatus,
		Entries:         entries,
	}
}

func (t *UpdateStorage) TryMerge(other Event) bool {
	event, ok := other.(*UpdateStorage)
	if !ok {
		return false
	}
	if event.StorageID != t.StorageID {
		return false
	}

	// 后投递的任务的状态更新一些
	t.DirectoryStatus = event.DirectoryStatus
	// TODO 可以考虑合并同FileHash和NodeID的记录
	t.Entries = append(t.Entries, event.Entries...)
	return true
}

func (t *UpdateStorage) Execute(execCtx ExecuteContext) {

	err := mysql.Storage.ChangeState(execCtx.Args.DB.SQLCtx(), t.StorageID, t.DirectoryStatus)
	if err != nil {
		logger.WithField("StorageID", t.StorageID).Warnf("change storage state failed, err: %s", err.Error())
	}

	for _, entry := range t.Entries {
		switch entry.Operation {
		case tskcst.UPDATE_STORAGE_DELETE:
			err := mysql.StorageObject.Delete(execCtx.Args.DB.SQLCtx(), t.StorageID, entry.ObjectID, entry.UserID)
			if err != nil {
				logger.WithField("StorageID", t.StorageID).
					WithField("ObjectID", entry.ObjectID).
					Warnf("delete storage object failed, err: %s", err.Error())
			}

		case tskcst.UPDATE_STORAGE_SET_NORMAL:
			err := mysql.StorageObject.SetStateNormal(execCtx.Args.DB.SQLCtx(), t.StorageID, entry.ObjectID, entry.UserID)
			if err != nil {
				logger.WithField("StorageID", t.StorageID).
					WithField("ObjectID", entry.ObjectID).
					Warnf("change storage object state failed, err: %s", err.Error())
			}
		}
	}
}
