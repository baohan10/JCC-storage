package cmd

import (
	"gitlink.org.cn/cloudream/common/consts"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
)

func (svc *Service) GetState(msg *agtmsg.GetState) (*agtmsg.GetStateResp, *ramsg.CodeMessage) {
	var ipfsState string

	if svc.ipfs.IsUp() {
		ipfsState = consts.IPFS_STATE_OK
	} else {
		ipfsState = consts.IPFS_STATE_OK
	}

	return ramsg.ReplyOK(agtmsg.NewGetStateRespBody(ipfsState))
}
