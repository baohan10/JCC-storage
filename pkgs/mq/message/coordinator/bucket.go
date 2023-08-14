package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type GetUserBuckets struct {
	UserID int64 `json:"userID"`
}

func NewGetUserBuckets(userID int64) GetUserBuckets {
	return GetUserBuckets{
		UserID: userID,
	}
}

type GetUserBucketsResp struct {
	Buckets []model.Bucket `json:"buckets"`
}

func NewGetUserBucketsResp(buckets []model.Bucket) GetUserBucketsResp {
	return GetUserBucketsResp{
		Buckets: buckets,
	}
}

type GetBucketObjects struct {
	UserID   int64 `json:"userID"`
	BucketID int64 `json:"bucketID"`
}

func NewGetBucketObjects(userID int64, bucketID int64) GetBucketObjects {
	return GetBucketObjects{
		UserID:   userID,
		BucketID: bucketID,
	}
}

type GetBucketObjectsResp struct {
	Objects []model.Object `json:"objects"`
}

func NewGetBucketObjectsResp(objects []model.Object) GetBucketObjectsResp {
	return GetBucketObjectsResp{
		Objects: objects,
	}
}

type CreateBucket struct {
	UserID     int64  `json:"userID"`
	BucketName string `json:"bucketName"`
}

func NewCreateBucket(userID int64, bucketName string) CreateBucket {
	return CreateBucket{
		UserID:     userID,
		BucketName: bucketName,
	}
}

type CreateBucketResp struct {
	BucketID int64 `json:"bucketID"`
}

func NewCreateBucketResp(bucketID int64) CreateBucketResp {
	return CreateBucketResp{
		BucketID: bucketID,
	}
}

type DeleteBucket struct {
	UserID   int64 `json:"userID"`
	BucketID int64 `json:"bucketID"`
}

func NewDeleteBucket(userID int64, bucketID int64) DeleteBucket {
	return DeleteBucket{
		UserID:   userID,
		BucketID: bucketID,
	}
}

type DeleteBucketResp struct{}

func NewDeleteBucketResp() DeleteBucketResp {
	return DeleteBucketResp{}
}

func init() {
	mq.RegisterMessage[GetUserBuckets]()
	mq.RegisterMessage[GetUserBucketsResp]()

	mq.RegisterMessage[GetBucketObjects]()
	mq.RegisterMessage[GetBucketObjectsResp]()

	mq.RegisterMessage[CreateBucket]()
	mq.RegisterMessage[CreateBucketResp]()

	mq.RegisterMessage[DeleteBucket]()
	mq.RegisterMessage[DeleteBucketResp]()
}
