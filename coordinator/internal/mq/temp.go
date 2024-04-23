package mq

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetDatabaseAll(msg *coormq.GetDatabaseAll) (*coormq.GetDatabaseAllResp, *mq.CodeMessage) {
	var bkts []cdssdk.Bucket
	var pkgs []cdssdk.Package
	var objs []stgmod.ObjectDetail

	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		var err error
		bkts, err = svc.db.Bucket().GetUserBuckets(tx, msg.UserID)
		if err != nil {
			return fmt.Errorf("get user buckets: %w", err)
		}

		for _, bkt := range bkts {
			ps, err := svc.db.Package().GetBucketPackages(tx, msg.UserID, bkt.BucketID)
			if err != nil {
				return fmt.Errorf("get bucket packages: %w", err)
			}
			pkgs = append(pkgs, ps...)
		}

		for _, pkg := range pkgs {
			os, err := svc.db.Object().GetPackageObjectDetails(tx, pkg.PackageID)
			if err != nil {
				return fmt.Errorf("get package object details: %w", err)
			}
			objs = append(objs, os...)
		}

		return nil
	})
	if err != nil {
		logger.Warnf("batch deleting objects: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch delete objects failed")
	}

	return mq.ReplyOK(coormq.RespGetDatabaseAll(bkts, pkgs, objs))
}
