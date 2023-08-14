package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

func (client *Client) StartPinningObject(msg agtmsg.StartPinningObject, opts ...mq.RequestOption) (*agtmsg.StartPinningObjectResp, error) {
	return mq.Request[agtmsg.StartPinningObjectResp](client.rabbitCli, msg, opts...)
}

func (client *Client) WaitPinningObject(msg agtmsg.WaitPinningObject, opts ...mq.RequestOption) (*agtmsg.WaitPinningObjectResp, error) {
	return mq.Request[agtmsg.WaitPinningObjectResp](client.rabbitCli, msg, opts...)
}
