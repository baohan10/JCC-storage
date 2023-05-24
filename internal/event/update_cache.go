package event

import (
	evtcst "gitlink.org.cn/cloudream/common/consts/event"
	"gitlink.org.cn/cloudream/common/utils/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
)

type UpdateCacheEntry struct {
	FileHash  string
	Operation string
}

func NewUpdateCacheEntry(fileHash string, op string) UpdateCacheEntry {
	return UpdateCacheEntry{
		FileHash:  fileHash,
		Operation: op,
	}
}

type UpdateCache struct {
	NodeID  int
	Entries []UpdateCacheEntry
}

func NewUpdateCache(nodeID int, entries []UpdateCacheEntry) UpdateCache {
	return UpdateCache{
		NodeID:  nodeID,
		Entries: entries,
	}
}

func (t *UpdateCache) TryMerge(other Event) bool {
	event, ok := other.(*UpdateCache)
	if !ok {
		return false
	}
	if event.NodeID != t.NodeID {
		return false
	}

	// TODO 可以考虑合并同FileHash和NodeID的记录
	t.Entries = append(t.Entries, event.Entries...)
	return true
}

func (t *UpdateCache) Execute(execCtx ExecuteContext) {
	for _, entry := range t.Entries {
		switch entry.Operation {
		case evtcst.UPDATE_CACHE_UNTEMP:
			err := mysql.Cache.DeleteTemp(execCtx.Args.DB.SQLCtx(), entry.FileHash, t.NodeID)

			if err != nil {
				logger.WithField("FileHash", entry.FileHash).
					WithField("NodeID", t.NodeID).
					Warnf("delete temp cache failed, err: %s", err.Error())
			}

		case evtcst.UPDATE_CACHE_CREATE_TEMP:
			err := mysql.Cache.CreateTemp(execCtx.Args.DB.SQLCtx(), entry.FileHash, t.NodeID)
			if err != nil {
				logger.WithField("FileHash", entry.FileHash).
					WithField("NodeID", t.NodeID).
					Warnf("create temp cache failed, err: %s", err.Error())
			}
		}
	}
}
