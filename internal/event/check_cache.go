package event

import (
	"database/sql"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db/model"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

type CheckCache struct {
	scevt.CheckCache
}

func NewCheckCache(nodeID int) *CheckCache {
	return &CheckCache{
		CheckCache: scevt.NewCheckCache(nodeID),
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
	log := logger.WithType[AgentCheckStorage]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t))

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 查询节点状态
		Node().ReadOne(t.NodeID).
		// 删除节点所有的Cache记录
		Cache().WriteAny().
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	node, err := execCtx.Args.DB.Node().GetByID(execCtx.Args.DB.SQLCtx(), t.NodeID)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("get node failed, err: %s", err.Error())
		return
	}

	if node.State != consts.NODE_STATE_UNAVAILABLE {
		return
	}

	caches, err := execCtx.Args.DB.Cache().GetNodeCaches(execCtx.Args.DB.SQLCtx(), t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("get node caches failed, err: %s", err.Error())
		return
	}

	err = execCtx.Args.DB.Cache().DeleteNodeAll(execCtx.Args.DB.SQLCtx(), t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("delete node all caches failed, err: %s", err.Error())
		return
	}

	execCtx.Executor.Post(NewCheckRepCount(lo.Map(caches, func(ch model.Cache, index int) string { return ch.FileHash })))
}

func init() {
	RegisterMessageConvertor(func(msg scevt.CheckCache) Event { return NewCheckCache(msg.NodeID) })
}
