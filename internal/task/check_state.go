package task

import (
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/common/consts"
	scmsg "gitlink.org.cn/cloudream/rabbitmq/message/scanner"
	sctsk "gitlink.org.cn/cloudream/rabbitmq/message/scanner/task"
)

type CheckStateTask struct {
}

func (t *CheckStateTask) TryMerge(other Task) bool {
	_, ok := other.(*CheckStateTask)
	return ok
}

func (t *CheckStateTask) Execute(execCtx *ExecuteContext, execOpts ExecuteOption) {
	ipfsStatus := consts.IPFS_STATUS_OK

	if execCtx.IPFS.IsUp() {
		ipfsStatus = consts.IPFS_STATUS_OK
	}

	// 紧急任务
	execCtx.Scanner.PostTask(scmsg.NewPostTaskBody(sctsk.NewUpdateAgentState(config.Cfg().ID, ipfsStatus), true, true))
}
