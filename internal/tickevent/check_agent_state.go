package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkg/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

type CheckAgentState struct {
}

func NewCheckAgentState() *CheckAgentState {
	return &CheckAgentState{}
}

func (e *CheckAgentState) Execute(ctx ExecuteContext) {
	log := logger.WithType[CheckAgentState]("TickEvent")
	log.Debugf("begin")

	nodes, err := mysql.Node.GetAllNodes(ctx.Args.DB.SQLCtx())
	if err != nil {
		log.Warnf("get all nodes failed, err: %s", err.Error())
		return
	}

	for _, node := range nodes {
		ctx.Args.EventExecutor.Post(event.NewAgentCheckState(node.NodeID), event.ExecuteOption{
			IsEmergency: true,
			DontMerge:   true,
		})
	}
}
