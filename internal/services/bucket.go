package services

import (
	log "github.com/sirupsen/logrus"
	"gitlink.org.cn/cloudream/db/model"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
)

func (svc *Service) GetBucket(userID int, bucketID int) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *Service) GetUserBuckets(msg *coormsg.GetUserBucketsCommand) *coormsg.GetUserBucketsResp {
	buckets, err := svc.db.GetUserBuckets(msg.UserID)

	if err != nil {
		log.WithField("UserID", msg.UserID).
			Warnf("get user buckets failed, err: %s", err.Error())
		return coormsg.NewGetUserBucketsRespFailed(errorcode.OPERATION_FAILED, "get all buckets failed")
	}

	return coormsg.NewGetUserBucketsRespOK(buckets)
}

func (svc *Service) GetBucketObjects(msg *coormsg.GetBucketObjectsCommand) *coormsg.GetBucketObjectsResp {
	objects, err := svc.db.GetBucketObjects(msg.UserID, msg.BucketID)

	if err != nil {
		log.WithField("UserID", msg.UserID).
			WithField("BucketID", msg.BucketID).
			Warnf("get bucket objects failed, err: %s", err.Error())
		return coormsg.NewGetBucketObjectsRespFailed(errorcode.OPERATION_FAILED, "get all buckets failed")
	}

	return coormsg.NewGetBucketObjectsRespOK(objects)
}

func (svc *Service) CreateBucket(msg *coormsg.CreateBucketCommand) *coormsg.CreateBucketResp {
	bucketID, err := svc.db.CreateBucket(msg.UserID, msg.BucketName)

	if err != nil {
		log.WithField("UserID", msg.UserID).
			WithField("BucketName", msg.BucketName).
			Warnf("create bucket failed, err: %s", err.Error())
		return coormsg.NewCreateBucketRespFailed(errorcode.OPERATION_FAILED, "create bucket failed")
	}

	return coormsg.NewCreateBucketRespOK(bucketID)
}

func (src *Service) DeleteBucket(userID int, bucketID int) error {
	// TODO
	panic("not implement yet")
}
