package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

func (client *Client) GetObjectsByDirName(msg coormsg.GetObjectsByDirName) (*coormsg.GetObjectsResp, error) {
	return mq.Request[coormsg.GetObjectsResp](client.rabbitCli, msg)
}

func (client *Client) PreDownloadObject(msg coormsg.PreDownloadObject) (*coormsg.PreDownloadObjectResp, error) {
	return mq.Request[coormsg.PreDownloadObjectResp](client.rabbitCli, msg)
}

func (client *Client) PreUploadRepObject(msg coormsg.PreUploadRepObject) (*coormsg.PreUploadResp, error) {
	return mq.Request[coormsg.PreUploadResp](client.rabbitCli, msg)
}

func (client *Client) CreateRepObject(msg coormsg.CreateRepObject) (*coormsg.CreateObjectResp, error) {
	return mq.Request[coormsg.CreateObjectResp](client.rabbitCli, msg)
}

func (client *Client) PreUploadEcObject(msg coormsg.PreUploadEcObject) (*coormsg.PreUploadEcResp, error) {
	return mq.Request[coormsg.PreUploadEcResp](client.rabbitCli, msg)
}

func (client *Client) CreateEcObject(msg coormsg.CreateEcObject) (*coormsg.CreateObjectResp, error) {
	return mq.Request[coormsg.CreateObjectResp](client.rabbitCli, msg)
}

func (client *Client) PreUpdateRepObject(msg coormsg.PreUpdateRepObject) (*coormsg.PreUpdateRepObjectResp, error) {
	return mq.Request[coormsg.PreUpdateRepObjectResp](client.rabbitCli, msg)
}

func (client *Client) UpdateRepObject(msg coormsg.UpdateRepObject) (*coormsg.UpdateRepObjectResp, error) {
	return mq.Request[coormsg.UpdateRepObjectResp](client.rabbitCli, msg)
}

func (client *Client) DeleteObject(msg coormsg.DeleteObject) (*coormsg.DeleteObjectResp, error) {
	return mq.Request[coormsg.DeleteObjectResp](client.rabbitCli, msg)
}
