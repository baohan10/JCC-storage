package services

import (
	"fmt"

	"gitlink.org.cn/cloudream/db/model"
)

func (svc *Service) GetBucket(userID int, bucketID int) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *Service) GetUserBuckets(userID int) ([]model.Bucket, error) {
	resp, err := svc.coordinator.GetUserBuckets(userID)
	if err != nil {
		return nil, fmt.Errorf("get user buckets failed, err: %w", err)
	}

	return resp.Buckets, nil
}

func (svc *Service) GetBucketObjects(userID int, bucketID int) ([]model.Object, error) {
	resp, err := svc.coordinator.GetBucketObjects(userID, bucketID)
	if err != nil {
		return nil, fmt.Errorf("get bucket objects failed, err: %w", err)
	}

	return resp.Objects, nil
}

func (svc *Service) CreateBucket(userID int, bucketName string) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (src *Service) DeleteBucket(userID int, bucketID int) error {
	// TODO
	panic("not implement yet")
}
