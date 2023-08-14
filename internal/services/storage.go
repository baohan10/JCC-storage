package services

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkg/logger"

	//"gitlink.org.cn/cloudream/common/utils"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

func (svc *Service) GetStorageInfo(msg *coormsg.GetStorageInfo) (*coormsg.GetStorageInfoResp, *ramsg.CodeMessage) {
	stg, err := svc.db.Storage().GetUserStorage(svc.db.SQLCtx(), msg.UserID, msg.StorageID)
	if err != nil {
		logger.Warnf("getting user storage: %s", err.Error())
		return nil, ramsg.Failed(errorcode.OperationFailed, "get user storage failed")
	}

	return ramsg.ReplyOK(coormsg.NewGetStorageInfoResp(stg.StorageID, stg.Name, stg.NodeID, stg.Directory, stg.State))
}

func (svc *Service) PreMoveObjectToStorage(msg *coormsg.PreMoveObjectToStorage) (*coormsg.PreMoveObjectToStorageResp, *ramsg.CodeMessage) {
	// 查询用户关联的存储服务
	stg, err := svc.db.Storage().GetUserStorage(svc.db.SQLCtx(), msg.UserID, msg.StorageID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("StorageID", msg.StorageID).
			Warnf("get user Storage failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OperationFailed, "get user Storage failed")
	}

	// 查询文件对象
	object, err := svc.db.Object().GetUserObject(svc.db.SQLCtx(), msg.UserID, msg.ObjectID)
	if err != nil {
		logger.WithField("ObjectID", msg.ObjectID).
			Warnf("get user Object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OperationFailed, "get user Object failed")
	}

	//-若redundancy是rep，查询对象副本表, 获得FileHash
	if object.Redundancy == models.RedundancyRep {
		objectRep, err := svc.db.ObjectRep().GetByID(svc.db.SQLCtx(), object.ObjectID)
		if err != nil {
			logger.Warnf("get ObjectRep failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OperationFailed, "get ObjectRep failed")
		}

		return ramsg.ReplyOK(coormsg.NewPreMoveObjectToStorageRespBody(
			stg.NodeID,
			stg.Directory,
			object,
			models.NewRedundancyRepData(objectRep.FileHash),
		))

	} else {
		// TODO 以EC_开头的Redundancy才是EC策略
		ecName := object.Redundancy
		blocks, err := svc.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			logger.WithField("ObjectID", object.ObjectID).
				Warnf("query Blocks failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OperationFailed, "query Blocks failed")
		}
		//查询纠删码参数
		ec, err := svc.db.Ec().GetEc(svc.db.SQLCtx(), ecName)
		// TODO zkx 异常处理
		ecc := models.NewEc(ec.EcID, ec.Name, ec.EcK, ec.EcN)

		blockss := make([]models.ObjectBlock, len(blocks))
		for i := 0; i < len(blocks); i++ {
			blockss[i] = models.NewObjectBlock(
				blocks[i].InnerID,
				blocks[i].BlockHash,
			)
		}

		return ramsg.ReplyOK(coormsg.NewPreMoveObjectToStorageRespBody(
			stg.NodeID,
			stg.Directory,
			object,
			models.NewRedundancyEcData(ecc, blockss),
		))
	}
}

func (svc *Service) MoveObjectToStorage(msg *coormsg.MoveObjectToStorage) (*coormsg.MoveObjectToStorageResp, *ramsg.CodeMessage) {
	// TODO: 对于的storage中已经存在的文件，直接覆盖已有文件
	err := svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		return svc.db.StorageObject().MoveObjectTo(tx, msg.ObjectID, msg.StorageID, msg.UserID)
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("ObjectID", msg.ObjectID).
			WithField("StorageID", msg.StorageID).
			Warnf("user move object to storage failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.MoveObjectToStorageResp](errorcode.OperationFailed, "user move object to storage failed")
	}

	return ramsg.ReplyOK(coormsg.NewMoveObjectToStorageResp())
}
