package services

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetBucket(userID cdssdk.UserID, bucketID cdssdk.BucketID) (model.Bucket, error) {
	// TODO
	panic("not implement yet")
}

func (svc *Service) GetUserBuckets(msg *coormq.GetUserBuckets) (*coormq.GetUserBucketsResp, *mq.CodeMessage) {
	buckets, err := svc.db.Bucket().GetUserBuckets(svc.db.SQLCtx(), msg.UserID)

	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warnf("get user buckets failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get all buckets failed")
	}

	return mq.ReplyOK(coormq.NewGetUserBucketsResp(buckets))
}

func (svc *Service) GetBucketPackages(msg *coormq.GetBucketPackages) (*coormq.GetBucketPackagesResp, *mq.CodeMessage) {
	packages, err := svc.db.Package().GetBucketPackages(svc.db.SQLCtx(), msg.UserID, msg.BucketID)

	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketID", msg.BucketID).
			Warnf("get bucket packages failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get bucket packages failed")
	}

	return mq.ReplyOK(coormq.NewGetBucketPackagesResp(packages))
}

func (svc *Service) CreateBucket(msg *coormq.CreateBucket) (*coormq.CreateBucketResp, *mq.CodeMessage) {
	var bucketID cdssdk.BucketID
	err := svc.db.DoTx(sql.LevelLinearizable, func(tx *sqlx.Tx) error {
		_, err := svc.db.User().GetByID(tx, msg.UserID)
		if err != nil {
			return fmt.Errorf("getting user by id: %w", err)
		}

		bucketID, err = svc.db.Bucket().Create(tx, msg.UserID, msg.BucketName)
		if err != nil {
			return fmt.Errorf("creating bucket: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketName", msg.BucketName).
			Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "create bucket failed")
	}

	return mq.ReplyOK(coormq.NewCreateBucketResp(bucketID))
}

func (svc *Service) DeleteBucket(msg *coormq.DeleteBucket) (*coormq.DeleteBucketResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelLinearizable, func(tx *sqlx.Tx) error {
		isAvai, _ := svc.db.Bucket().IsAvailable(tx, msg.BucketID, msg.UserID)
		if !isAvai {
			return fmt.Errorf("bucket is not avaiable to the user")
		}

		err := svc.db.Bucket().Delete(tx, msg.BucketID)
		if err != nil {
			return fmt.Errorf("deleting bucket: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("BucketID", msg.BucketID).
			Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "delete bucket failed")
	}

	return mq.ReplyOK(coormq.NewDeleteBucketResp())
}
