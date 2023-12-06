package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type BucketService interface {
	GetUserBuckets(msg *GetUserBuckets) (*GetUserBucketsResp, *mq.CodeMessage)

	GetBucketPackages(msg *GetBucketPackages) (*GetBucketPackagesResp, *mq.CodeMessage)

	CreateBucket(msg *CreateBucket) (*CreateBucketResp, *mq.CodeMessage)

	DeleteBucket(msg *DeleteBucket) (*DeleteBucketResp, *mq.CodeMessage)
}

// 获取用户所有的桶
var _ = Register(Service.GetUserBuckets)

type GetUserBuckets struct {
	mq.MessageBodyBase
	UserID cdssdk.UserID `json:"userID"`
}
type GetUserBucketsResp struct {
	mq.MessageBodyBase
	Buckets []model.Bucket `json:"buckets"`
}

func NewGetUserBuckets(userID cdssdk.UserID) *GetUserBuckets {
	return &GetUserBuckets{
		UserID: userID,
	}
}
func NewGetUserBucketsResp(buckets []model.Bucket) *GetUserBucketsResp {
	return &GetUserBucketsResp{
		Buckets: buckets,
	}
}
func (client *Client) GetUserBuckets(msg *GetUserBuckets) (*GetUserBucketsResp, error) {
	return mq.Request(Service.GetUserBuckets, client.rabbitCli, msg)
}

// 获取桶中的所有Package
var _ = Register(Service.GetBucketPackages)

type GetBucketPackages struct {
	mq.MessageBodyBase
	UserID   cdssdk.UserID   `json:"userID"`
	BucketID cdssdk.BucketID `json:"bucketID"`
}
type GetBucketPackagesResp struct {
	mq.MessageBodyBase
	Packages []model.Package `json:"packages"`
}

func NewGetBucketPackages(userID cdssdk.UserID, bucketID cdssdk.BucketID) *GetBucketPackages {
	return &GetBucketPackages{
		UserID:   userID,
		BucketID: bucketID,
	}
}
func NewGetBucketPackagesResp(packages []model.Package) *GetBucketPackagesResp {
	return &GetBucketPackagesResp{
		Packages: packages,
	}
}
func (client *Client) GetBucketPackages(msg *GetBucketPackages) (*GetBucketPackagesResp, error) {
	return mq.Request(Service.GetBucketPackages, client.rabbitCli, msg)
}

// 创建桶
var _ = Register(Service.CreateBucket)

type CreateBucket struct {
	mq.MessageBodyBase
	UserID     cdssdk.UserID `json:"userID"`
	BucketName string        `json:"bucketName"`
}
type CreateBucketResp struct {
	mq.MessageBodyBase
	BucketID cdssdk.BucketID `json:"bucketID"`
}

func NewCreateBucket(userID cdssdk.UserID, bucketName string) *CreateBucket {
	return &CreateBucket{
		UserID:     userID,
		BucketName: bucketName,
	}
}
func NewCreateBucketResp(bucketID cdssdk.BucketID) *CreateBucketResp {
	return &CreateBucketResp{
		BucketID: bucketID,
	}
}
func (client *Client) CreateBucket(msg *CreateBucket) (*CreateBucketResp, error) {
	return mq.Request(Service.CreateBucket, client.rabbitCli, msg)
}

// 删除桶
var _ = Register(Service.DeleteBucket)

type DeleteBucket struct {
	mq.MessageBodyBase
	UserID   cdssdk.UserID   `json:"userID"`
	BucketID cdssdk.BucketID `json:"bucketID"`
}
type DeleteBucketResp struct {
	mq.MessageBodyBase
}

func NewDeleteBucket(userID cdssdk.UserID, bucketID cdssdk.BucketID) *DeleteBucket {
	return &DeleteBucket{
		UserID:   userID,
		BucketID: bucketID,
	}
}
func NewDeleteBucketResp() *DeleteBucketResp {
	return &DeleteBucketResp{}
}
func (client *Client) DeleteBucket(msg *DeleteBucket) (*DeleteBucketResp, error) {
	return mq.Request(Service.DeleteBucket, client.rabbitCli, msg)
}
