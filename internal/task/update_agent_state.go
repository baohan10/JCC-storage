package task

import (
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/utils/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
)

type UpdateAgentStateTask struct {
	NodeID     int
	IPFSStatus string
}

func (t *UpdateAgentStateTask) TryMerge(other Task) bool {
	return false
}

func (t *UpdateAgentStateTask) Execute(execCtx *ExecuteContext, execOpts ExecuteOption) {
	if t.IPFSStatus != consts.IPFS_STATUS_OK {
		logger.WithField("NodeID", t.NodeID).Warnf("IPFS status is %s, set node state unavailable", t.IPFSStatus)

		err := mysql.Node.ChangeState(execCtx.DB.SQLCtx(), t.NodeID, consts.NODE_STATE_UNAVAILABLE)
		if err != nil {
			logger.WithField("NodeID", t.NodeID).Warnf("change node state failed, err: %s", err.Error())
		}
		return
	}

	// TODO 如果以后还有其他的状态，要判断哪些状态下能设置Normal
	err := mysql.Node.ChangeState(execCtx.DB.SQLCtx(), t.NodeID, consts.NODE_STATE_NORMAL)
	if err != nil {
		logger.WithField("NodeID", t.NodeID).Warnf("change node state failed, err: %s", err.Error())
	}
}
