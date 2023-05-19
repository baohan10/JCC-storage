package task

import (
	tskcst "gitlink.org.cn/cloudream/common/consts/task"
	mysql "gitlink.org.cn/cloudream/db/sql"
	"gitlink.org.cn/cloudream/utils/logger"
)

type UpdateCacheTaskEntry struct {
	FileHash  string
	NodeID    int
	Operation string
}

func NewUpdateCacheTaskEntry(fileHash string, nodeID int, op string) UpdateCacheTaskEntry {
	return UpdateCacheTaskEntry{
		FileHash:  fileHash,
		NodeID:    nodeID,
		Operation: op,
	}
}

type UpdateCacheTask struct {
	Entries []UpdateCacheTaskEntry
}

func NewUpdateCacheTask(entries []UpdateCacheTaskEntry) UpdateCacheTask {
	return UpdateCacheTask{
		Entries: entries,
	}
}

func (t *UpdateCacheTask) TryMerge(other Task) bool {
	chkTask, ok := other.(*UpdateCacheTask)
	if !ok {
		return false
	}

	// TODO 可以考虑合并同FileHash和NodeID的记录
	t.Entries = append(t.Entries, chkTask.Entries...)
	return true
}

func (t *UpdateCacheTask) Execute(execCtx *ExecuteContext, execOpts ExecuteOption) {
	for _, entry := range t.Entries {
		switch entry.Operation {
		case tskcst.UPDATE_CACHE_OP_UNTEMP:
			err := mysql.Cache.DeleteTemp(execCtx.DB.SQLCtx(), entry.FileHash, entry.NodeID)

			if err != nil {
				logger.WithField("FileHash", entry.FileHash).
					WithField("NodeID", entry.NodeID).
					Warnf("delete temp cache failed, err: %s", err.Error())
			}
		}
	}
}
