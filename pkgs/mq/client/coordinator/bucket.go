package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

func (client *Client) GetUserBuckets(msg coormsg.GetUserBuckets) (*coormsg.GetUserBucketsResp, error) {
	return mq.Request[coormsg.GetUserBucketsResp](client.rabbitCli, msg)
}

func (client *Client) GetBucketObjects(msg coormsg.GetBucketObjects) (*coormsg.GetBucketObjectsResp, error) {
	return mq.Request[coormsg.GetBucketObjectsResp](client.rabbitCli, msg)
}

func (client *Client) CreateBucket(msg coormsg.CreateBucket) (*coormsg.CreateBucketResp, error) {
	return mq.Request[coormsg.CreateBucketResp](client.rabbitCli, msg)
}

func (client *Client) DeleteBucket(msg coormsg.DeleteBucket) (*coormsg.DeleteBucketResp, error) {
	return mq.Request[coormsg.DeleteBucketResp](client.rabbitCli, msg)
}
