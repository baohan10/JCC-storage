package task

import (
	tskcst "gitlink.org.cn/cloudream/common/consts/task"
	"gitlink.org.cn/cloudream/common/utils/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
)

type UpdateStorageTaskEntry struct {
	ObjectID  int
	Operation string
}

func NewUpdateStorageTaskEntry(objectID int, op string) UpdateStorageTaskEntry {
	return UpdateStorageTaskEntry{
		ObjectID:  objectID,
		Operation: op,
	}
}

type UpdateStorageTask struct {
	StorageID       int
	DirectoryStatus string
	Entries         []UpdateStorageTaskEntry
}

func NewUpdateStorageTask(dirStatus string, entries []UpdateStorageTaskEntry) UpdateStorageTask {
	return UpdateStorageTask{
		DirectoryStatus: dirStatus,
		Entries:         entries,
	}
}

func (t *UpdateStorageTask) TryMerge(other Task) bool {
	task, ok := other.(*UpdateStorageTask)
	if !ok {
		return false
	}
	if task.StorageID != t.StorageID {
		return false
	}

	// 后投递的任务的状态更新一些
	t.DirectoryStatus = task.DirectoryStatus
	// TODO 可以考虑合并同FileHash和NodeID的记录
	t.Entries = append(t.Entries, task.Entries...)
	return true
}

func (t *UpdateStorageTask) Execute(execCtx *ExecuteContext, execOpts ExecuteOption) {

	err := mysql.Storage.ChangeState(execCtx.DB.SQLCtx(), t.StorageID, t.DirectoryStatus)
	if err != nil {
		logger.WithField("StorageID", t.StorageID).Warnf("change storage state failed, err: %s", err.Error())
	}

	for _, entry := range t.Entries {
		switch entry.Operation {
		case tskcst.UPDATE_STORAGE_DELETE:
			err := mysql.StorageObject.Delete(execCtx.DB.SQLCtx(), t.StorageID, entry.ObjectID)
			if err != nil {
				logger.WithField("StorageID", t.StorageID).
					WithField("ObjectID", entry.ObjectID).
					Warnf("delete storage object failed, err: %s", err.Error())
			}

		case tskcst.UPDATE_STORAGE_SET_NORMAL:
			err := mysql.StorageObject.SetStateNormal(execCtx.DB.SQLCtx(), t.StorageID, entry.ObjectID)
			if err != nil {
				logger.WithField("StorageID", t.StorageID).
					WithField("ObjectID", entry.ObjectID).
					Warnf("change storage object state failed, err: %s", err.Error())
			}
		}
	}
}
