package event

import (
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	agtevt "gitlink.org.cn/cloudream/rabbitmq/message/agent/event"
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
	log := logger.WithType[CheckState]("Event")
	log.Debugf("begin")

	ipfsStatus := consts.IPFS_STATUS_OK

	if execCtx.Args.IPFS.IsUp() {
		ipfsStatus = consts.IPFS_STATUS_OK
	}

	// 紧急任务
	err := execCtx.Args.Scanner.PostEvent(scevt.NewUpdateAgentState(config.Cfg().ID, ipfsStatus), true, true)
	if err != nil {
		log.Warnf("post event to scanner failed, err: %s", err.Error())
	}
}

func init() {
	Register(func(val agtevt.CheckState) Event { return NewCheckState() })
}
