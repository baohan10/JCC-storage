package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
)

type CheckAgentState struct {
}

func NewCheckAgentState() *CheckAgentState {
	return &CheckAgentState{}
}

func (e *CheckAgentState) Execute(ctx ExecuteContext) {
	log := logger.WithType[CheckAgentState]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	nodes, err := ctx.Args.DB.Node().GetAllNodes(ctx.Args.DB.SQLCtx())
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
