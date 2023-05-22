package event

import (
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/common/consts"
	scmsg "gitlink.org.cn/cloudream/rabbitmq/message/scanner"
	sctsk "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

type CheckState struct {
}

func (t *CheckState) TryMerge(other Event) bool {
	_, ok := other.(*CheckState)
	return ok
}

func (t *CheckState) Execute(execCtx ExecuteContext) {
	ipfsStatus := consts.IPFS_STATUS_OK

	if execCtx.Args.IPFS.IsUp() {
		ipfsStatus = consts.IPFS_STATUS_OK
	}

	// 紧急任务
	execCtx.Args.Scanner.PostEvent(scmsg.NewPostEventBody(sctsk.NewUpdateAgentState(config.Cfg().ID, ipfsStatus), true, true))
}
