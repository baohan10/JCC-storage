package services

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type BucketService struct {
	*Service
}

func (svc *Service) BucketSvc() *BucketService {
	return &BucketService{Service: svc}
}

func (svc *BucketService) GetBucket(userID cdssdk.UserID, bucketID cdssdk.BucketID) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *BucketService) GetBucketByName(userID cdssdk.UserID, bucketName string) (model.Bucket, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return model.Bucket{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.GetBucketByName(coormq.ReqGetBucketByName(userID, bucketName))
	if err != nil {
		return model.Bucket{}, fmt.Errorf("get bucket by name failed, err: %w", err)
	}

	return resp.Bucket, nil
}

func (svc *BucketService) GetUserBuckets(userID cdssdk.UserID) ([]model.Bucket, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.GetUserBuckets(coormq.NewGetUserBuckets(userID))
	if err != nil {
		return nil, fmt.Errorf("get user buckets failed, err: %w", err)
	}

	return resp.Buckets, nil
}

func (svc *BucketService) GetBucketPackages(userID cdssdk.UserID, bucketID cdssdk.BucketID) ([]model.Package, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.GetBucketPackages(coormq.NewGetBucketPackages(userID, bucketID))
	if err != nil {
		return nil, fmt.Errorf("get bucket packages failed, err: %w", err)
	}

	return resp.Packages, nil
}

func (svc *BucketService) CreateBucket(userID cdssdk.UserID, bucketName string) (cdssdk.Bucket, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return cdssdk.Bucket{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.CreateBucket(coormq.NewCreateBucket(userID, bucketName))
	if err != nil {
		return cdssdk.Bucket{}, fmt.Errorf("creating bucket: %w", err)
	}

	return resp.Bucket, nil
}

func (svc *BucketService) DeleteBucket(userID cdssdk.UserID, bucketID cdssdk.BucketID) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	_, err = coorCli.DeleteBucket(coormq.NewDeleteBucket(userID, bucketID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}

	return nil
}
