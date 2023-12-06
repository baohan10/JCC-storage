package services

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) CachePackageMoved(msg *coormq.CachePackageMoved) (*coormq.CachePackageMovedResp, *mq.CodeMessage) {
	if err := svc.db.Cache().SetPackageObjectFrozen(svc.db.SQLCtx(), msg.PackageID, msg.NodeID); err != nil {
		logger.Warnf("setting package object frozen: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "set package object frozen failed")
	}

	return mq.ReplyOK(coormq.NewCachePackageMovedResp())
}
