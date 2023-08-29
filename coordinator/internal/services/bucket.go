package services

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

func (svc *Service) GetBucket(userID int, bucketID int) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *Service) GetUserBuckets(msg *coormq.GetUserBuckets) (*coormq.GetUserBucketsResp, *mq.CodeMessage) {
	buckets, err := svc.db.Bucket().GetUserBuckets(svc.db.SQLCtx(), msg.UserID)

	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warnf("get user buckets failed, err: %s", err.Error())
		return mq.ReplyFailed[coormq.GetUserBucketsResp](errorcode.OperationFailed, "get all buckets failed")
	}

	return mq.ReplyOK(coormq.NewGetUserBucketsResp(buckets))
}

func (svc *Service) GetBucketPackages(msg *coormq.GetBucketPackages) (*coormq.GetBucketPackagesResp, *mq.CodeMessage) {
	packages, err := svc.db.Package().GetBucketPackages(svc.db.SQLCtx(), msg.UserID, msg.BucketID)

	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketID", msg.BucketID).
			Warnf("get bucket packages failed, err: %s", err.Error())
		return mq.ReplyFailed[coormq.GetBucketPackagesResp](errorcode.OperationFailed, "get bucket packages failed")
	}

	return mq.ReplyOK(coormq.NewGetBucketPackagesResp(packages))
}

func (svc *Service) CreateBucket(msg *coormq.CreateBucket) (*coormq.CreateBucketResp, *mq.CodeMessage) {
	var bucketID int64
	var err error
	svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		// 这里用的是外部的err
		bucketID, err = svc.db.Bucket().Create(tx, msg.UserID, msg.BucketName)
		return err
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketName", msg.BucketName).
			Warnf("create bucket failed, err: %s", err.Error())
		return mq.ReplyFailed[coormq.CreateBucketResp](errorcode.OperationFailed, "create bucket failed")
	}

	return mq.ReplyOK(coormq.NewCreateBucketResp(bucketID))
}

func (svc *Service) DeleteBucket(msg *coormq.DeleteBucket) (*coormq.DeleteBucketResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		return svc.db.Bucket().Delete(tx, msg.BucketID)
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketID", msg.BucketID).
			Warnf("delete bucket failed, err: %s", err.Error())
		return mq.ReplyFailed[coormq.DeleteBucketResp](errorcode.OperationFailed, "delete bucket failed")
	}

	return mq.ReplyOK(coormq.NewDeleteBucketResp())
}
