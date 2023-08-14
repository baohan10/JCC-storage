package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

type AgentService interface {
	GetState(msg *agtmsg.GetState) (*agtmsg.GetStateResp, *mq.CodeMessage)
}

func init() {
	Register(AgentService.GetState)
}
