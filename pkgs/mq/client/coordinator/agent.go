package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

func (client *Client) TempCacheReport(msg coormsg.TempCacheReport) error {
	return mq.Send(client.rabbitCli, msg)
}

func (client *Client) AgentStatusReport(msg coormsg.AgentStatusReport) error {
	return mq.Send(client.rabbitCli, msg)
}
