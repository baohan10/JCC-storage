package services

import (
	"fmt"

	"gitlink.org.cn/cloudream/db/model"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

type BucketService struct {
	*Service
}

func BucketSvc(svc *Service) *BucketService {
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
	if !resp.IsOK() {
		return nil, fmt.Errorf("create bucket objects failed, code: %s, message: %s", resp.ErrorCode, resp.Message)
	}

	return resp.Body.Buckets, nil
}

func (svc *BucketService) GetBucketObjects(userID int, bucketID int) ([]model.Object, error) {
	resp, err := svc.coordinator.GetBucketObjects(coormsg.NewGetBucketObjectsBody(userID, bucketID))
	if err != nil {
		return nil, fmt.Errorf("get bucket objects failed, err: %w", err)
	}
	if !resp.IsOK() {
		return nil, fmt.Errorf("create bucket objects failed, code: %s, message: %s", resp.ErrorCode, resp.Message)
	}

	return resp.Body.Objects, nil
}

func (svc *BucketService) CreateBucket(userID int, bucketName string) (int, error) {
	resp, err := svc.coordinator.CreateBucket(coormsg.NewCreateBucketBody(userID, bucketName))
	if err != nil {
		return 0, fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if !resp.IsOK() {
		return 0, fmt.Errorf("create bucket objects failed, code: %s, message: %s", resp.ErrorCode, resp.Message)
	}

	return resp.Body.BucketID, nil
}

func (svc *BucketService) DeleteBucket(userID int, bucketID int) error {
	resp, err := svc.coordinator.DeleteBucket(coormsg.NewDeleteBucketBody(userID, bucketID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if !resp.IsOK() {
		return fmt.Errorf("delete bucket failed, code: %s, message: %s", resp.ErrorCode, resp.Message)
	}

	return nil
}
