package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

type IPFSService interface {
	CheckIPFS(msg *agtmsg.CheckIPFS) (*agtmsg.CheckIPFSResp, *mq.CodeMessage)
}

func init() {
	Register(IPFSService.CheckIPFS)
}
