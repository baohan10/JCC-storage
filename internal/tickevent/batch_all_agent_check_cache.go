package tickevent

import (
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db/model"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

const AGENT_CHECK_CACHE_BATCH_SIZE = 2

type BatchAllAgentCheckCache struct {
	nodeIDs []int64
}

func NewBatchAllAgentCheckCache() *BatchAllAgentCheckCache {
	return &BatchAllAgentCheckCache{}
}

func (e *BatchAllAgentCheckCache) Execute(ctx ExecuteContext) {
	log := logger.WithType[BatchAllAgentCheckCache]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	if e.nodeIDs == nil || len(e.nodeIDs) == 0 {
		nodes, err := ctx.Args.DB.Node().GetAllNodes(ctx.Args.DB.SQLCtx())
		if err != nil {
			log.Warnf("get all nodes failed, err: %s", err.Error())
			return
		}

		e.nodeIDs = lo.Map(nodes, func(node model.Node, index int) int64 { return node.NodeID })

		log.Debugf("new check start, get all nodes")
	}

	checkedCnt := 0
	for ; checkedCnt < len(e.nodeIDs) && checkedCnt < AGENT_CHECK_CACHE_BATCH_SIZE; checkedCnt++ {
		// nil代表进行全量检查
		ctx.Args.EventExecutor.Post(event.NewAgentCheckCache(e.nodeIDs[checkedCnt], nil))
	}
	e.nodeIDs = e.nodeIDs[checkedCnt:]
}
