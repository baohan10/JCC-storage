package services

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db/model"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

func (svc *Service) GetBucket(userID int, bucketID int) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *Service) GetUserBuckets(msg *coormsg.GetUserBuckets) *coormsg.GetUserBucketsResp {
	buckets, err := svc.db.Bucket().GetUserBuckets(svc.db.SQLCtx(), msg.Body.UserID)

	if err != nil {
		log.WithField("UserID", msg.Body.UserID).
			Warnf("get user buckets failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.GetUserBucketsResp](errorcode.OPERATION_FAILED, "get all buckets failed")
	}

	return ramsg.ReplyOK(coormsg.NewGetUserBucketsRespBody(buckets))
}

func (svc *Service) GetBucketObjects(msg *coormsg.GetBucketObjects) *coormsg.GetBucketObjectsResp {
	objects, err := svc.db.Object().GetBucketObjects(svc.db.SQLCtx(), msg.Body.UserID, msg.Body.BucketID)

	if err != nil {
		log.WithField("UserID", msg.Body.UserID).
			WithField("BucketID", msg.Body.BucketID).
			Warnf("get bucket objects failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.GetBucketObjectsResp](errorcode.OPERATION_FAILED, "get bucket objects failed")
	}

	return ramsg.ReplyOK(coormsg.NewGetBucketObjectsRespBody(objects))
}

func (svc *Service) CreateBucket(msg *coormsg.CreateBucket) *coormsg.CreateBucketResp {
	bucketID, err := svc.db.Bucket().Create(svc.db.SQLCtx(), msg.Body.UserID, msg.Body.BucketName)

	if err != nil {
		log.WithField("UserID", msg.Body.UserID).
			WithField("BucketName", msg.Body.BucketName).
			Warnf("create bucket failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.CreateBucketResp](errorcode.OPERATION_FAILED, "create bucket failed")
	}

	return ramsg.ReplyOK(coormsg.NewCreateBucketRespBody(bucketID))
}

func (svc *Service) DeleteBucket(msg *coormsg.DeleteBucket) *coormsg.DeleteBucketResp {
	err := svc.db.Bucket().Delete(svc.db.SQLCtx(), msg.Body.BucketID)

	if err != nil {
		log.WithField("UserID", msg.Body.UserID).
			WithField("BucketID", msg.Body.BucketID).
			Warnf("delete bucket failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.DeleteBucketResp](errorcode.OPERATION_FAILED, "delete bucket failed")
	}

	return ramsg.ReplyOK(coormsg.NewDeleteBucketRespBody())
}
