package event

import (
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

type UpdateAgentState struct {
	scevt.UpdateAgentState
}

func NewUpdateAgentState(nodeID int, ipfsState string) *UpdateAgentState {
	return &UpdateAgentState{
		UpdateAgentState: scevt.NewUpdateAgentState(nodeID, ipfsState),
	}
}

func (t *UpdateAgentState) TryMerge(other Event) bool {
	return false
}

func (t *UpdateAgentState) Execute(execCtx ExecuteContext) {
	logger.Debugf("begin update agent state")

	if t.IPFSState != consts.IPFS_STATUS_OK {
		logger.WithField("NodeID", t.NodeID).Warnf("IPFS status is %s, set node state unavailable", t.IPFSState)

		err := mysql.Node.ChangeState(execCtx.Args.DB.SQLCtx(), t.NodeID, consts.NODE_STATE_UNAVAILABLE)
		if err != nil {
			logger.WithField("NodeID", t.NodeID).Warnf("change node state failed, err: %s", err.Error())
		}
		return
	}

	// TODO 如果以后还有其他的状态，要判断哪些状态下能设置Normal
	err := mysql.Node.ChangeState(execCtx.Args.DB.SQLCtx(), t.NodeID, consts.NODE_STATE_NORMAL)
	if err != nil {
		logger.WithField("NodeID", t.NodeID).Warnf("change node state failed, err: %s", err.Error())
	}
}

func init() {
	RegisterMessageConvertor(func(msg scevt.UpdateAgentState) Event { return NewUpdateAgentState(msg.NodeID, msg.IPFSState) })
}
