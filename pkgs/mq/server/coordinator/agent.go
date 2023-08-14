package coordinator

import coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"

type AgentService interface {
	TempCacheReport(msg *coormsg.TempCacheReport)

	AgentStatusReport(msg *coormsg.AgentStatusReport)
}

func init() {
	RegisterNoReply(AgentService.TempCacheReport)

	RegisterNoReply(AgentService.AgentStatusReport)
}
