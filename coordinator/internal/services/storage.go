package services

import (
	"database/sql"

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
	// TODO: 对于的storage中已经存在的文件，直接覆盖已有文件
	err := svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		return svc.db.StoragePackage().LoadPackage(tx, msg.PackageID, msg.StorageID, msg.UserID)
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			WithField("StorageID", msg.StorageID).
			Warnf("user load package to storage failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "user load package to storage failed")
	}

	return mq.ReplyOK(coormq.NewStoragePackageLoadedResp())
}
