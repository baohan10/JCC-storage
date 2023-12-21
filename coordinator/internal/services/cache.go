package services

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) CachePackageMoved(msg *coormq.CachePackageMoved) (*coormq.CachePackageMovedResp, *mq.CodeMessage) {
	if err := svc.db.PinnedObject().CreateFromPackage(svc.db.SQLCtx(), msg.PackageID, msg.NodeID); err != nil {
		logger.Warnf("create package pinned objects: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "create package pinned objects failed")
	}

	return mq.ReplyOK(coormq.NewCachePackageMovedResp())
}
