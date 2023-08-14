package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

type ObjectService interface {
	StartPinningObject(msg *agtmsg.StartPinningObject) (*agtmsg.StartPinningObjectResp, *mq.CodeMessage)
	WaitPinningObject(msg *agtmsg.WaitPinningObject) (*agtmsg.WaitPinningObjectResp, *mq.CodeMessage)
}

func init() {
	Register(ObjectService.StartPinningObject)
	Register(ObjectService.WaitPinningObject)
}
