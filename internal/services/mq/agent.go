package mq

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/consts"
	"gitlink.org.cn/cloudream/storage-common/globals"
	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
)

func (svc *Service) GetState(msg *agtmq.GetState) (*agtmq.GetStateResp, *mq.CodeMessage) {
	var ipfsState string

	ipfsCli, err := globals.IPFSPool.Acquire()
	if err != nil {
		logger.Warnf("new ipfs client: %s", err.Error())
		ipfsState = consts.IPFSStateUnavailable

	} else {
		if ipfsCli.IsUp() {
			ipfsState = consts.IPFSStateOK
		} else {
			ipfsState = consts.IPFSStateUnavailable
		}
		ipfsCli.Close()
	}

	return mq.ReplyOK(agtmq.NewGetStateResp(ipfsState))
}
