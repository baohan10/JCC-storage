package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

func (client *Client) GetStorageInfo(msg coormsg.GetStorageInfo) (*coormsg.GetStorageInfoResp, error) {
	return mq.Request[coormsg.GetStorageInfoResp](client.rabbitCli, msg)
}

func (client *Client) PreMoveObjectToStorage(msg coormsg.PreMoveObjectToStorage) (*coormsg.PreMoveObjectToStorageResp, error) {
	return mq.Request[coormsg.PreMoveObjectToStorageResp](client.rabbitCli, msg)
}

func (client *Client) MoveObjectToStorage(msg coormsg.MoveObjectToStorage) (*coormsg.MoveObjectToStorageResp, error) {
	return mq.Request[coormsg.MoveObjectToStorageResp](client.rabbitCli, msg)
}
