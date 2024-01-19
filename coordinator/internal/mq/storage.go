package mq

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"

	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetStorageInfo(msg *coormq.GetStorageInfo) (*coormq.GetStorageInfoResp, *mq.CodeMessage) {
	stg, err := svc.db.Storage().GetUserStorage(svc.db.SQLCtx(), msg.UserID, msg.StorageID)
	if err != nil {
		logger.Warnf("getting user storage: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get user storage failed")
	}

	return mq.ReplyOK(coormq.NewGetStorageInfoResp(stg.StorageID, stg.Name, stg.NodeID, stg.Directory, stg.State))
}

func (svc *Service) StoragePackageLoaded(msg *coormq.StoragePackageLoaded) (*coormq.StoragePackageLoadedResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		// 可以不用检查用户是否存在
		if ok, _ := svc.db.Package().IsAvailable(tx, msg.UserID, msg.PackageID); !ok {
			return fmt.Errorf("package is not available to user")
		}

		if ok, _ := svc.db.Storage().IsAvailable(tx, msg.UserID, msg.StorageID); !ok {
			return fmt.Errorf("storage is not available to user")
		}

		err := svc.db.StoragePackage().Create(tx, msg.StorageID, msg.PackageID, msg.UserID)
		if err != nil {
			return fmt.Errorf("creating storage package: %w", err)
		}

		err = svc.db.StoragePackageLog().Create(tx, msg.StorageID, msg.PackageID, msg.UserID, time.Now())
		if err != nil {
			return fmt.Errorf("creating storage package log: %w", err)
		}

		stg, err := svc.db.Storage().GetByID(tx, msg.StorageID)
		if err != nil {
			return fmt.Errorf("getting storage: %w", err)
		}

		err = svc.db.PinnedObject().CreateFromPackage(tx, msg.PackageID, stg.NodeID)
		if err != nil {
			return fmt.Errorf("creating pinned object from package: %w", err)
		}

		if len(msg.PinnedBlocks) > 0 {
			err = svc.db.ObjectBlock().BatchCreate(tx, msg.PinnedBlocks)
			if err != nil {
				return fmt.Errorf("batch creating object block: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("StorageID", msg.StorageID).
			WithField("PackageID", msg.PackageID).
			Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "user load package to storage failed")
	}

	return mq.ReplyOK(coormq.NewStoragePackageLoadedResp())
}
