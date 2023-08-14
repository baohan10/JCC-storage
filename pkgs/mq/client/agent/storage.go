package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

func (client *Client) StartStorageMoveObject(msg agtmsg.StartStorageMoveObject, opts ...mq.RequestOption) (*agtmsg.StartStorageMoveObjectResp, error) {
	return mq.Request[agtmsg.StartStorageMoveObjectResp](client.rabbitCli, msg, opts...)
}

func (client *Client) WaitStorageMoveObject(msg agtmsg.WaitStorageMoveObject, opts ...mq.RequestOption) (*agtmsg.WaitStorageMoveObjectResp, error) {
	return mq.Request[agtmsg.WaitStorageMoveObjectResp](client.rabbitCli, msg, opts...)
}

func (client *Client) StorageCheck(msg agtmsg.StorageCheck, opts ...mq.RequestOption) (*agtmsg.StorageCheckResp, error) {
	return mq.Request[agtmsg.StorageCheckResp](client.rabbitCli, msg, opts...)
}

func (client *Client) StartStorageUploadRepObject(msg agtmsg.StartStorageUploadRepObject, opts ...mq.RequestOption) (*agtmsg.StartStorageUploadRepObjectResp, error) {
	return mq.Request[agtmsg.StartStorageUploadRepObjectResp](client.rabbitCli, msg, opts...)
}

func (client *Client) WaitStorageUploadRepObject(msg agtmsg.WaitStorageUploadRepObject, opts ...mq.RequestOption) (*agtmsg.WaitStorageUploadRepObjectResp, error) {
	return mq.Request[agtmsg.WaitStorageUploadRepObjectResp](client.rabbitCli, msg, opts...)
}
