package event

import (
	evtcst "gitlink.org.cn/cloudream/common/consts/event"
	"gitlink.org.cn/cloudream/common/utils/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

type UpdateCacheEntry = scevt.UpdateCacheEntry

type UpdateCache struct {
	scevt.UpdateCache
}

func NewUpdateCache(nodeID int, entries []scevt.UpdateCacheEntry) *UpdateCache {
	return &UpdateCache{
		UpdateCache: scevt.NewUpdateCache(nodeID, entries),
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
	logger.Debugf("begin update cache")

	for _, entry := range t.Entries {
		switch entry.Operation {
		case evtcst.UPDATE_CACHE_DELETE_TEMP:
			err := mysql.Cache.DeleteTemp(execCtx.Args.DB.SQLCtx(), entry.FileHash, t.NodeID)
			if err != nil {
				logger.WithField("FileHash", entry.FileHash).
					WithField("NodeID", t.NodeID).
					Warnf("delete temp cache failed, err: %s", err.Error())
			}

			logger.WithField("FileHash", entry.FileHash).
				WithField("NodeID", t.NodeID).
				Debugf("delete temp cache")

		case evtcst.UPDATE_CACHE_CREATE_TEMP:
			err := mysql.Cache.CreateTemp(execCtx.Args.DB.SQLCtx(), entry.FileHash, t.NodeID)
			if err != nil {
				logger.WithField("FileHash", entry.FileHash).
					WithField("NodeID", t.NodeID).
					Warnf("create temp cache failed, err: %s", err.Error())
			}

			logger.WithField("FileHash", entry.FileHash).
				WithField("NodeID", t.NodeID).
				Debugf("create temp cache")
		}
	}
}

func init() {
	RegisterMessageConvertor(func(msg scevt.UpdateCache) Event { return NewUpdateCache(msg.NodeID, msg.Entries) })
}
