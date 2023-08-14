package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

func (client *Client) GetState(msg agtmsg.GetState, opts ...mq.RequestOption) (*agtmsg.GetStateResp, error) {
	return mq.Request[agtmsg.GetStateResp](client.rabbitCli, msg, opts...)
}
