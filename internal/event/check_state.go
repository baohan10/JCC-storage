package event

import (
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/utils/logger"
	agtevt "gitlink.org.cn/cloudream/rabbitmq/message/agent/event"
	scmsg "gitlink.org.cn/cloudream/rabbitmq/message/scanner"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

type CheckState struct {
}

func NewCheckState() *CheckState {
	return &CheckState{}
}

func (t *CheckState) TryMerge(other Event) bool {
	_, ok := other.(*CheckState)
	return ok
}

func (t *CheckState) Execute(execCtx ExecuteContext) {
	logger.Debugf("begin check state")

	ipfsStatus := consts.IPFS_STATUS_OK

	if execCtx.Args.IPFS.IsUp() {
		ipfsStatus = consts.IPFS_STATUS_OK
	}

	// 紧急任务
	evtmsg, err := scmsg.NewPostEventBody(scevt.NewUpdateAgentState(config.Cfg().ID, ipfsStatus), true, true)
	if err == nil {
		execCtx.Args.Scanner.PostEvent(evtmsg)
	} else {
		logger.Warnf("new post event body failed, err: %s", err.Error())
	}
}

func init() {
	Register(func(val agtevt.CheckState) Event { return NewCheckState() })
}
