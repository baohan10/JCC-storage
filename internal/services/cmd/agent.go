package cmd

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/consts"
	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
)

func (svc *Service) GetState(msg *agtmq.GetState) (*agtmq.GetStateResp, *mq.CodeMessage) {
	var ipfsState string

	if svc.ipfs.IsUp() {
		ipfsState = consts.IPFSStateOK
	} else {
		ipfsState = consts.IPFSStateOK
	}

	return mq.ReplyOK(agtmq.NewGetStateResp(ipfsState))
}
