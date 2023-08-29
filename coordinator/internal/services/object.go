package services

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

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
