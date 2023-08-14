package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

type BucketService interface {
	GetUserBuckets(msg *coormsg.GetUserBuckets) (*coormsg.GetUserBucketsResp, *mq.CodeMessage)

	GetBucketObjects(msg *coormsg.GetBucketObjects) (*coormsg.GetBucketObjectsResp, *mq.CodeMessage)

	CreateBucket(msg *coormsg.CreateBucket) (*coormsg.CreateBucketResp, *mq.CodeMessage)

	DeleteBucket(msg *coormsg.DeleteBucket) (*coormsg.DeleteBucketResp, *mq.CodeMessage)
}

func init() {
	Register(BucketService.GetUserBuckets)

	Register(BucketService.GetBucketObjects)

	Register(BucketService.CreateBucket)

	Register(BucketService.DeleteBucket)
}
