package services

import "gitlink.org.cn/cloudream/db/model"

func (svc *Service) GetBucket(userID int, bucketID int) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *Service) GetAllBuckets(userID int) ([]model.Bucket, error) {
}

func (svc *Service) GetBucketObjects(userID int, bucketID int) ([]model.Object, error) {

}

func (svc *Service) CreateBucket(userID int, bucketName string) (model.Bucket, error) {

}

func (src *Service) DeleteBucket(userID int, bucketID int) error {

}
