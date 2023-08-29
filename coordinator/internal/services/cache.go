package services

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) CachePackageMoved(msg *coormq.CachePackageMoved) (*coormq.CachePackageMovedResp, *mq.CodeMessage) {
	pkg, err := svc.db.Package().GetByID(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("getting package: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get package failed")
	}

	if pkg.Redundancy.IsRepInfo() {
		// TODO 优先级
		if err := svc.db.Cache().BatchCreatePinned(svc.db.SQLCtx(), msg.FileHashes, msg.NodeID, 0); err != nil {
			logger.Warnf("batch creating pinned cache: %s", err.Error())
			return nil, mq.Failed(errorcode.OperationFailed, "batch create pinned cache failed")
		}
	}

	// TODO EC的逻辑

	return mq.ReplyOK(coormq.NewCachePackageMovedResp())
}
