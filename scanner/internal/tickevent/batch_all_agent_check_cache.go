package tickevent

import (
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
)

const AGENT_CHECK_CACHE_BATCH_SIZE = 2

type BatchAllAgentCheckCache struct {
	nodeIDs []cdssdk.NodeID
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

		e.nodeIDs = lo.Map(nodes, func(node cdssdk.Node, index int) cdssdk.NodeID { return node.NodeID })

		log.Debugf("new check start, get all nodes")
	}

	checkedCnt := 0
	for ; checkedCnt < len(e.nodeIDs) && checkedCnt < AGENT_CHECK_CACHE_BATCH_SIZE; checkedCnt++ {
		// nil代表进行全量检查
		ctx.Args.EventExecutor.Post(event.NewAgentCheckCache(scevt.NewAgentCheckCache(e.nodeIDs[checkedCnt])))
	}
	e.nodeIDs = e.nodeIDs[checkedCnt:]
}
