package cmd

import (
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/mq"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

func (svc *Service) GetState(msg *agtmsg.GetState) (*agtmsg.GetStateResp, *mq.CodeMessage) {
	var ipfsState string

	if svc.ipfs.IsUp() {
		ipfsState = consts.IPFSStateOK
	} else {
		ipfsState = consts.IPFSStateOK
	}

	return mq.ReplyOK(agtmsg.NewGetStateRespBody(ipfsState))
}
