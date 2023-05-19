package task

import (
	tskcst "gitlink.org.cn/cloudream/common/consts/task"
	mysql "gitlink.org.cn/cloudream/db/sql"
	"gitlink.org.cn/cloudream/utils/logger"
)

type UpdateCacheTaskEntry struct {
	FileHash  string
	Operation string
}

func NewUpdateCacheTaskEntry(fileHash string, op string) UpdateCacheTaskEntry {
	return UpdateCacheTaskEntry{
		FileHash:  fileHash,
		Operation: op,
	}
}

type UpdateCacheTask struct {
	NodeID  int
	Entries []UpdateCacheTaskEntry
}

func NewUpdateCacheTask(nodeID int, entries []UpdateCacheTaskEntry) UpdateCacheTask {
	return UpdateCacheTask{
		NodeID:  nodeID,
		Entries: entries,
	}
}

func (t *UpdateCacheTask) TryMerge(other Task) bool {
	task, ok := other.(*UpdateCacheTask)
	if !ok {
		return false
	}
	if task.NodeID != t.NodeID {
		return false
	}

	// TODO 可以考虑合并同FileHash和NodeID的记录
	t.Entries = append(t.Entries, task.Entries...)
	return true
}

func (t *UpdateCacheTask) Execute(execCtx *ExecuteContext, execOpts ExecuteOption) {
	for _, entry := range t.Entries {
		switch entry.Operation {
		case tskcst.UPDATE_CACHE_OP_UNTEMP:
			err := mysql.Cache.DeleteTemp(execCtx.DB.SQLCtx(), entry.FileHash, t.NodeID)

			if err != nil {
				logger.WithField("FileHash", entry.FileHash).
					WithField("NodeID", t.NodeID).
					Warnf("delete temp cache failed, err: %s", err.Error())
			}
		}
	}
}
