package services

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetPackageObjectCacheInfos(msg *coormq.GetPackageObjectCacheInfos) (*coormq.GetPackageObjectCacheInfosResp, *mq.CodeMessage) {
	pkg, err := svc.db.Package().GetUserPackage(svc.db.SQLCtx(), msg.UserID, msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("getting package: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package failed")
	}

	if pkg.Redundancy.IsRepInfo() {
		infos, err := svc.db.ObjectRep().GetPackageObjectCacheInfos(svc.db.SQLCtx(), msg.PackageID)
		if err != nil {
			logger.WithField("PackageID", msg.PackageID).
				Warnf("getting rep package object cache infos: %s", err.Error())

			return nil, mq.Failed(errorcode.OperationFailed, "get rep package object cache infos failed")
		}

		return mq.ReplyOK(coormq.NewGetPackageObjectCacheInfosResp(infos))
	}
	// TODO EC

	return nil, mq.Failed(errorcode.OperationFailed, "not implement yet")
}

func (svc *Service) GetPackageObjectRepData(msg *coormq.GetPackageObjectRepData) (*coormq.GetPackageObjectRepDataResp, *mq.CodeMessage) {
	data, err := svc.db.ObjectRep().GetWithNodeIDInPackage(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("query object rep and node id in package: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "query object rep and node id in package failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageObjectRepDataResp(data))
}

func (svc *Service) GetPackageObjectECData(msg *coormq.GetPackageObjectECData) (*coormq.GetPackageObjectECDataResp, *mq.CodeMessage) {
	data, err := svc.db.ObjectBlock().GetWithNodeIDInPackage(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("query object ec and node id in package: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "query object ec and node id in package failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageObjectECDataResp(data))
}
