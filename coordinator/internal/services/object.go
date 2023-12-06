package services

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetPackageObjects(msg *coormq.GetPackageObjects) (*coormq.GetPackageObjectsResp, *mq.CodeMessage) {
	// TODO 检查用户是否有权限
	objs, err := svc.db.Object().GetPackageObjects(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package objects: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package objects failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageObjectsResp(objs))
}

func (svc *Service) GetPackageObjectDetails(msg *coormq.GetPackageObjectDetails) (*coormq.GetPackageObjectDetailsResp, *mq.CodeMessage) {
	data, err := svc.db.ObjectBlock().GetPackageBlockDetails(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("getting package block details: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package object block details failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageObjectDetailsResp(data))
}
