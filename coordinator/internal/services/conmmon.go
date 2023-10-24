package services

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) FindClientLocation(msg *coormq.FindClientLocation) (*coormq.FindClientLocationResp, *mq.CodeMessage) {
	location, err := svc.db.Location().FindLocationByExternalIP(svc.db.SQLCtx(), msg.IP)
	if err != nil {
		logger.WithField("IP", msg.IP).
			Warnf("finding location by external ip: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "query client location failed")
	}

	return mq.ReplyOK(coormq.NewFindClientLocationResp(location))
}

func (svc *Service) GetECConfig(msg *coormq.GetECConfig) (*coormq.GetECConfigResp, *mq.CodeMessage) {
	ec, err := svc.db.Ec().GetEc(svc.db.SQLCtx(), msg.ECName)
	if err != nil {
		logger.WithField("ECName", msg.ECName).
			Warnf("query ec failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "query ec failed")
	}

	return mq.ReplyOK(coormq.NewGetECConfigResp(ec))
}
