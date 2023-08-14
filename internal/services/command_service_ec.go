package services

import (
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mymq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

func (svc *Service) PreUploadEcObject(msg *coormsg.PreUploadEcObject) (*coormsg.PreUploadEcResp, *mq.CodeMessage) {

	// 判断同名对象是否存在。等到UploadRepObject时再判断一次。
	// 此次的判断只作为参考，具体是否成功还是看UploadRepObject的结果
	isBucketAvai, err := svc.db.Bucket().IsAvailable(svc.db.SQLCtx(), msg.BucketID, msg.UserID)
	if err != nil {
		logger.WithField("BucketID", msg.BucketID).
			Warnf("check bucket available failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUploadEcResp](errorcode.OperationFailed, "check bucket available failed")
	}
	if !isBucketAvai {
		logger.WithField("BucketID", msg.BucketID).
			Warnf("bucket is not available to user")
		return mq.ReplyFailed[coormsg.PreUploadEcResp](errorcode.OperationFailed, "bucket is not available to user")
	}

	_, err = svc.db.Object().GetByName(svc.db.SQLCtx(), msg.BucketID, msg.ObjectName)
	if err == nil {
		logger.WithField("BucketID", msg.BucketID).
			WithField("ObjectName", msg.ObjectName).
			Warnf("object with given Name and BucketID already exists")
		return mq.ReplyFailed[coormsg.PreUploadEcResp](errorcode.OperationFailed, "object with given Name and BucketID already exists")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		logger.WithField("BucketID", msg.BucketID).
			WithField("ObjectName", msg.ObjectName).
			Warnf("get object by name failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUploadEcResp](errorcode.OperationFailed, "get object by name failed")
	}

	//查询用户可用的节点IP
	nodes, err := svc.db.Node().GetUserNodes(svc.db.SQLCtx(), msg.UserID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warnf("query user nodes failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUploadEcResp](errorcode.OperationFailed, "query user nodes failed")
	}

	// 查询客户端所属节点
	foundBelongNode := true
	belongNode, err := svc.db.Node().GetByExternalIP(svc.db.SQLCtx(), msg.ClientExternalIP)
	if err == sql.ErrNoRows {
		foundBelongNode = false
	} else if err != nil {
		logger.WithField("ClientExternalIP", msg.ClientExternalIP).
			Warnf("query client belong node failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUploadEcResp](errorcode.OperationFailed, "query client belong node failed")
	}

	var respNodes []mymq.RespNode
	for _, node := range nodes {
		respNodes = append(respNodes, mymq.NewRespNode(
			node.NodeID,
			node.ExternalIP,
			node.LocalIP,
			// LocationID 相同则认为是在同一个地域
			foundBelongNode && belongNode.LocationID == node.LocationID,
		))
	}
	//查询纠删码参数
	ec, err := svc.db.Ec().GetEc(svc.db.SQLCtx(), msg.EcName)
	if err != nil {
		logger.WithField("Ec", msg.EcName).
			Warnf("check ec type failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUploadEcResp](errorcode.OperationFailed, "check bucket available failed")
	}
	ecc := mymq.NewEc(ec.EcID, ec.Name, ec.EcK, ec.EcN)
	return mq.ReplyOK(coormsg.NewPreUploadEcResp(respNodes, ecc))
}

func (svc *Service) CreateEcObject(msg *coormsg.CreateEcObject) (*coormsg.CreateObjectResp, *mq.CodeMessage) {
	var objID int64
	err := svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		var err error
		objID, err = svc.db.Object().CreateEcObject(tx, msg.BucketID, msg.ObjectName, msg.FileSize, msg.UserID, msg.NodeIDs, msg.Hashes, msg.EcName, msg.DirName)
		return err
	})
	if err != nil {
		logger.WithField("BucketName", msg.BucketID).
			WithField("ObjectName", msg.ObjectName).
			Warnf("create rep object failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.CreateObjectResp](errorcode.OperationFailed, "create rep object failed")
	}

	return mq.ReplyOK(coormsg.NewCreateObjectResp(objID))
}
