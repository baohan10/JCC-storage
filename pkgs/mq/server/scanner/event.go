package scanner

import scmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/scanner"

type EventService interface {
	PostEvent(event *scmsg.PostEvent)
}

func init() {
	RegisterNoReply(EventService.PostEvent)
}
