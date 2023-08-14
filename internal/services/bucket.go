package services

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/common/pkg/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

func (svc *Service) GetBucket(userID int, bucketID int) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *Service) GetUserBuckets(msg *coormsg.GetUserBuckets) (*coormsg.GetUserBucketsResp, *mq.CodeMessage) {
	buckets, err := svc.db.Bucket().GetUserBuckets(svc.db.SQLCtx(), msg.UserID)

	if err != nil {
		log.WithField("UserID", msg.UserID).
			Warnf("get user buckets failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.GetUserBucketsResp](errorcode.OperationFailed, "get all buckets failed")
	}

	return mq.ReplyOK(coormsg.NewGetUserBucketsResp(buckets))
}

func (svc *Service) GetBucketObjects(msg *coormsg.GetBucketObjects) (*coormsg.GetBucketObjectsResp, *mq.CodeMessage) {
	objects, err := svc.db.Object().GetBucketObjects(svc.db.SQLCtx(), msg.UserID, msg.BucketID)

	if err != nil {
		log.WithField("UserID", msg.UserID).
			WithField("BucketID", msg.BucketID).
			Warnf("get bucket objects failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.GetBucketObjectsResp](errorcode.OperationFailed, "get bucket objects failed")
	}

	return mq.ReplyOK(coormsg.NewGetBucketObjectsResp(objects))
}

func (svc *Service) CreateBucket(msg *coormsg.CreateBucket) (*coormsg.CreateBucketResp, *mq.CodeMessage) {
	var bucketID int64
	var err error
	svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		// 这里用的是外部的err
		bucketID, err = svc.db.Bucket().Create(tx, msg.UserID, msg.BucketName)
		return err
	})
	if err != nil {
		log.WithField("UserID", msg.UserID).
			WithField("BucketName", msg.BucketName).
			Warnf("create bucket failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.CreateBucketResp](errorcode.OperationFailed, "create bucket failed")
	}

	return mq.ReplyOK(coormsg.NewCreateBucketResp(bucketID))
}

func (svc *Service) DeleteBucket(msg *coormsg.DeleteBucket) (*coormsg.DeleteBucketResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		return svc.db.Bucket().Delete(tx, msg.BucketID)
	})
	if err != nil {
		log.WithField("UserID", msg.UserID).
			WithField("BucketID", msg.BucketID).
			Warnf("delete bucket failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.DeleteBucketResp](errorcode.OperationFailed, "delete bucket failed")
	}

	return mq.ReplyOK(coormsg.NewDeleteBucketResp())
}
