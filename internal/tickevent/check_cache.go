package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

type CheckCache struct {
}

func NewCheckCache() *CheckCache {
	return &CheckCache{}
}

func (e *CheckCache) Execute(ctx ExecuteContext) {
	log := logger.WithType[CheckCache]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	nodes, err := ctx.Args.DB.Node().GetAllNodes(ctx.Args.DB.SQLCtx())
	if err != nil {
		log.Warnf("get all nodes failed, err: %s", err.Error())
		return
	}

	for _, node := range nodes {
		ctx.Args.EventExecutor.Post(event.NewCheckCache(node.NodeID))
	}
}
