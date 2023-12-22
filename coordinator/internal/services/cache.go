package services

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) CachePackageMoved(msg *coormq.CachePackageMoved) (*coormq.CachePackageMovedResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelLinearizable, func(tx *sqlx.Tx) error {
		_, err := svc.db.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		_, err = svc.db.Node().GetByID(tx, msg.NodeID)
		if err != nil {
			return fmt.Errorf("getting node by id: %w", err)
		}

		err = svc.db.PinnedObject().CreateFromPackage(tx, msg.PackageID, msg.NodeID)
		if err != nil {
			return fmt.Errorf("creating pinned objects from package: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).WithField("NodeID", msg.NodeID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "create package pinned objects failed")
	}

	return mq.ReplyOK(coormq.NewCachePackageMovedResp())
}
