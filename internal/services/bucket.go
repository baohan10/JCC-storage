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

func (svc *BucketService) GetBucket(userID int, bucketID int) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *BucketService) GetUserBuckets(userID int) ([]model.Bucket, error) {
	resp, err := svc.coordinator.GetUserBuckets(coormsg.NewGetUserBucketsBody(userID))
	if err != nil {
		return nil, fmt.Errorf("get user buckets failed, err: %w", err)
	}
	if resp.IsFailed() {
		return nil, fmt.Errorf("create bucket objects failed, code: %s, message: %s", resp.ErrorCode, resp.ErrorMessage)
	}

	return resp.Body.Buckets, nil
}

func (svc *BucketService) GetBucketObjects(userID int, bucketID int) ([]model.Object, error) {
	resp, err := svc.coordinator.GetBucketObjects(coormsg.NewGetBucketObjectsBody(userID, bucketID))
	if err != nil {
		return nil, fmt.Errorf("get bucket objects failed, err: %w", err)
	}
	if resp.IsFailed() {
		return nil, fmt.Errorf("create bucket objects failed, code: %s, message: %s", resp.ErrorCode, resp.ErrorMessage)
	}

	return resp.Body.Objects, nil
}

func (svc *BucketService) CreateBucket(userID int, bucketName string) (int, error) {
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

	resp, err := svc.coordinator.CreateBucket(coormsg.NewCreateBucketBody(userID, bucketName))
	if err != nil {
		return 0, fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if resp.IsFailed() {
		return 0, fmt.Errorf("create bucket objects failed, code: %s, message: %s", resp.ErrorCode, resp.ErrorMessage)
	}

	return resp.Body.BucketID, nil
}

func (svc *BucketService) DeleteBucket(userID int, bucketID int) error {
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

	resp, err := svc.coordinator.DeleteBucket(coormsg.NewDeleteBucketBody(userID, bucketID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if resp.IsFailed() {
		return fmt.Errorf("delete bucket failed, code: %s, message: %s", resp.ErrorCode, resp.ErrorMessage)
	}

	return nil
}
