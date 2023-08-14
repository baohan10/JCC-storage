package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

func (client *Client) CheckIPFS(msg agtmsg.CheckIPFS, opts ...mq.RequestOption) (*agtmsg.CheckIPFSResp, error) {
	return mq.Request[agtmsg.CheckIPFSResp](client.rabbitCli, msg, opts...)
}
