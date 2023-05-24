package event

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/utils/logger"
	"gitlink.org.cn/cloudream/db/model"
	mysql "gitlink.org.cn/cloudream/db/sql"
)

type CheckCache struct {
	NodeID int
}

func NewCheckCache(nodeID int) *CheckCache {
	return &CheckCache{
		NodeID: nodeID,
	}
}

func (t *CheckCache) TryMerge(other Event) bool {
	event, ok := other.(*CheckCache)
	if !ok {
		return false
	}
	if event.NodeID != t.NodeID {
		return false
	}

	return true
}

func (t *CheckCache) Execute(execCtx ExecuteContext) {
	err := execCtx.Args.DB.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		node, err := mysql.Node.GetByID(tx, t.NodeID)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return fmt.Errorf("get node failed, err: %w", err)
		}

		if node.State != consts.NODE_STATE_UNAVAILABLE {
			return nil
		}

		caches, err := mysql.Cache.GetNodeCaches(tx, t.NodeID)
		if err != nil {
			return fmt.Errorf("get node caches failed, err: %w", err)
		}

		err = mysql.Cache.DeleteNodeAll(tx, t.NodeID)
		if err != nil {
			return fmt.Errorf("delete node all caches failed, err: %w", err)
		}

		execCtx.Executor.Post(NewCheckRepCount(lo.Map(caches, func(ch model.Cache, index int) string { return ch.HashValue })))
		return nil
	})

	if err != nil {
		logger.WithField("NodeID", t.NodeID).Warn(err.Error())
	}
}

func init() {
	Register(func(msg CheckCache) Event { return NewCheckCache(msg.NodeID) })
}
