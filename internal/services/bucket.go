package services

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/db/model"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

type BucketService struct {
	*Service
}

func (svc *Service) BucketSvc() *BucketService {
	return &BucketService{Service: svc}
}

func (svc *BucketService) GetBucket(userID int64, bucketID int64) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *BucketService) GetUserBuckets(userID int64) ([]model.Bucket, error) {
	resp, err := svc.coordinator.GetUserBuckets(coormsg.NewGetUserBuckets(userID))
	if err != nil {
		return nil, fmt.Errorf("get user buckets failed, err: %w", err)
	}

	return resp.Buckets, nil
}

func (svc *BucketService) GetBucketObjects(userID int64, bucketID int64) ([]model.Object, error) {
	resp, err := svc.coordinator.GetBucketObjects(coormsg.NewGetBucketObjects(userID, bucketID))
	if err != nil {
		return nil, fmt.Errorf("get bucket objects failed, err: %w", err)
	}

	return resp.Objects, nil
}

func (svc *BucketService) CreateBucket(userID int64, bucketName string) (int64, error) {
	// TODO 只有阅读了系统操作的源码，才能知道要加哪些锁，但用户的命令可能会调用不止一个系统操作。
	// 因此加锁的操作还是必须在用户命令里完成，但具体加锁的内容，则需要被封装起来与系统操作放到一起，方便管理，避免分散改动。

	mutex, err := reqbuilder.NewBuilder().
		Metadata().Bucket().CreateOne(userID, bucketName).
		// TODO 可以考虑二次加锁，加的更精确
		UserBucket().CreateAny().
		MutexLock(svc.distlock)
	if err != nil {
		return 0, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	resp, err := svc.coordinator.CreateBucket(coormsg.NewCreateBucket(userID, bucketName))
	if err != nil {
		return 0, fmt.Errorf("creating bucket: %w", err)
	}

	return resp.BucketID, nil
}

func (svc *BucketService) DeleteBucket(userID int64, bucketID int64) error {
	// TODO 检查用户是否有删除这个Bucket的权限。检查的时候可以只上UserBucket的Read锁

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		UserBucket().WriteAny().
		Bucket().WriteOne(bucketID).
		Object().WriteAny().
		ObjectRep().WriteAny().
		ObjectBlock().WriteAny().
		StorageObject().WriteAny().
		MutexLock(svc.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	_, err = svc.coordinator.DeleteBucket(coormsg.NewDeleteBucket(userID, bucketID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}

	return nil
}
