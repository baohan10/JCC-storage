package mq

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
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
	var details []stgmod.ObjectDetail
	// 必须放在事务里进行，因为GetPackageBlockDetails是由多次数据库操作组成，必须保证数据的一致性
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		var err error
		_, err = svc.db.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		details, err = svc.db.Object().GetPackageObjectDetails(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package block details: %w", err)
		}

		return nil
	})

	if err != nil {
		logger.WithField("PackageID", msg.PackageID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get package object block details failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageObjectDetailsResp(details))
}

func (svc *Service) ChangeObjectRedundancy(msg *coormq.ChangeObjectRedundancy) (*coormq.ChangeObjectRedundancyResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		return svc.db.Object().BatchUpdateRedundancy(tx, msg.Entries)
	})
	if err != nil {
		logger.Warnf("batch updating redundancy: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch update redundancy failed")
	}

	return mq.ReplyOK(coormq.RespChangeObjectRedundancy())
}
