package services

import (
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	mymq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
	scevt "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/scanner/event"
)

func (svc *Service) GetObjectsByDirName(msg *coormsg.GetObjectsByDirName) (*coormsg.GetObjectsResp, *mq.CodeMessage) {
	//查询dirName下所有文件
	objects, err := svc.db.Object().GetByDirName(svc.db.SQLCtx(), msg.DirName)
	if err != nil {
		logger.WithField("DirName", msg.DirName).
			Warnf("query dirname failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.GetObjectsResp](errorcode.OperationFailed, "get objects failed")
	}

	return mq.ReplyOK(coormsg.NewGetObjectsResp(objects))
}

func (svc *Service) PreDownloadObject(msg *coormsg.PreDownloadObject) (*coormsg.PreDownloadObjectResp, *mq.CodeMessage) {

	// 查询文件对象
	object, err := svc.db.Object().GetUserObject(svc.db.SQLCtx(), msg.UserID, msg.ObjectID)
	if err != nil {
		logger.WithField("ObjectID", msg.ObjectID).
			Warnf("query Object failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OperationFailed, "query Object failed")
	}

	// 查询客户端所属节点
	foundBelongNode := true
	belongNode, err := svc.db.Node().GetByExternalIP(svc.db.SQLCtx(), msg.ClientExternalIP)
	if err == sql.ErrNoRows {
		foundBelongNode = false
	} else if err != nil {
		logger.WithField("ClientExternalIP", msg.ClientExternalIP).
			Warnf("query client belong node failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OperationFailed, "query client belong node failed")
	}
	logger.Debugf("client address %s is at location %d", msg.ClientExternalIP, belongNode.LocationID)

	//-若redundancy是rep，查询对象副本表, 获得FileHash
	if object.Redundancy == models.RedundancyRep {
		objectRep, err := svc.db.ObjectRep().GetByID(svc.db.SQLCtx(), object.ObjectID)
		if err != nil {
			logger.WithField("ObjectID", object.ObjectID).
				Warnf("get ObjectRep failed, err: %s", err.Error())
			return mq.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OperationFailed, "query ObjectRep failed")
		}

		// 注：由于采用了IPFS存储，因此每个备份文件的FileHash都是一样的
		nodes, err := svc.db.Cache().FindCachingFileUserNodes(svc.db.SQLCtx(), msg.UserID, objectRep.FileHash)
		if err != nil {
			logger.WithField("FileHash", objectRep.FileHash).
				Warnf("query Cache failed, err: %s", err.Error())
			return mq.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OperationFailed, "query Cache failed")
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

		return mq.ReplyOK(coormsg.NewPreDownloadObjectResp(
			object.FileSize,
			mymq.NewRespRepRedundancyData(objectRep.FileHash, respNodes),
		))

	} else {
		// TODO 参考上面进行重写
		ecName := object.Redundancy
		blocks, err := svc.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			logger.WithField("ObjectID", object.ObjectID).
				Warnf("query Blocks failed, err: %s", err.Error())
			return mq.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OperationFailed, "query Blocks failed")
		}
		logger.Debugf(blocks[4].BlockHash)
		//查询纠删码参数
		ec, err := svc.db.Ec().GetEc(svc.db.SQLCtx(), ecName)
		ecc := mymq.NewEc(ec.EcID, ec.Name, ec.EcK, ec.EcN)
		//查询每个编码块存放的所有节点
		respNodes := make([][]mymq.RespNode, len(blocks))
		for i := 0; i < len(blocks); i++ {
			nodes, err := svc.db.Cache().FindCachingFileUserNodes(svc.db.SQLCtx(), msg.UserID, blocks[i].BlockHash)
			if err != nil {
				logger.WithField("FileHash", blocks[i].BlockHash).
					Warnf("query Cache failed, err: %s", err.Error())
				return mq.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OperationFailed, "query Cache failed")
			}
			var nd []mymq.RespNode
			for _, node := range nodes {
				nd = append(nd, mymq.NewRespNode(
					node.NodeID,
					node.ExternalIP,
					node.LocalIP,
					// LocationID 相同则认为是在同一个地域
					foundBelongNode && belongNode.LocationID == node.LocationID,
				))
			}
			respNodes[i] = nd
			logger.Debugf("##%d\n", i)
		}

		var blockss []mymq.RespObjectBlock
		for i := 0; i < len(blocks); i++ {
			blockss = append(blockss, mymq.NewRespObjectBlock(
				blocks[i].InnerID,
				blocks[i].BlockHash,
			))
		}
		return mq.ReplyOK(coormsg.NewPreDownloadObjectResp(
			object.FileSize,
			mymq.NewRespEcRedundancyData(ecc, blockss, respNodes),
		))
	}
}

func (svc *Service) PreUploadRepObject(msg *coormsg.PreUploadRepObject) (*coormsg.PreUploadResp, *mq.CodeMessage) {

	// 判断同名对象是否存在。等到UploadRepObject时再判断一次。
	// 此次的判断只作为参考，具体是否成功还是看UploadRepObject的结果
	isBucketAvai, err := svc.db.Bucket().IsAvailable(svc.db.SQLCtx(), msg.BucketID, msg.UserID)
	if err != nil {
		logger.WithField("BucketID", msg.BucketID).
			Warnf("check bucket available failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUploadResp](errorcode.OperationFailed, "check bucket available failed")
	}
	if !isBucketAvai {
		logger.WithField("BucketID", msg.BucketID).
			Warnf("bucket is not available to user")
		return mq.ReplyFailed[coormsg.PreUploadResp](errorcode.OperationFailed, "bucket is not available to user")
	}

	_, err = svc.db.Object().GetByName(svc.db.SQLCtx(), msg.BucketID, msg.ObjectName)
	if err == nil {
		logger.WithField("BucketID", msg.BucketID).
			WithField("ObjectName", msg.ObjectName).
			Warnf("object with given Name and BucketID already exists")
		return mq.ReplyFailed[coormsg.PreUploadResp](errorcode.OperationFailed, "object with given Name and BucketID already exists")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		logger.WithField("BucketID", msg.BucketID).
			WithField("ObjectName", msg.ObjectName).
			Warnf("get object by name failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUploadResp](errorcode.OperationFailed, "get object by name failed")
	}

	//查询用户可用的节点IP
	nodes, err := svc.db.Node().GetUserNodes(svc.db.SQLCtx(), msg.UserID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warnf("query user nodes failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUploadResp](errorcode.OperationFailed, "query user nodes failed")
	}

	// 查询客户端所属节点
	foundBelongNode := true
	belongNode, err := svc.db.Node().GetByExternalIP(svc.db.SQLCtx(), msg.ClientExternalIP)
	if err == sql.ErrNoRows {
		foundBelongNode = false
	} else if err != nil {
		logger.WithField("ClientExternalIP", msg.ClientExternalIP).
			Warnf("query client belong node failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUploadResp](errorcode.OperationFailed, "query client belong node failed")
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

	return mq.ReplyOK(coormsg.NewPreUploadResp(respNodes))
}

func (svc *Service) CreateRepObject(msg *coormsg.CreateRepObject) (*coormsg.CreateObjectResp, *mq.CodeMessage) {
	var objID int64
	err := svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		var err error
		objID, err = svc.db.Object().CreateRepObject(tx, msg.BucketID, msg.ObjectName, msg.FileSize, msg.RepCount, msg.NodeIDs, msg.FileHash, msg.DirName)
		return err
	})
	if err != nil {
		logger.WithField("BucketName", msg.BucketID).
			WithField("ObjectName", msg.ObjectName).
			Warnf("create rep object failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.CreateObjectResp](errorcode.OperationFailed, "create rep object failed")
	}

	// 紧急任务
	err = svc.scanner.PostEvent(scevt.NewCheckRepCount([]string{msg.FileHash}), true, true)
	if err != nil {
		logger.Warnf("post event to scanner failed, but this will not affect creating, err: %s", err.Error())
	}

	return mq.ReplyOK(coormsg.NewCreateObjectResp(objID))
}

func (svc *Service) PreUpdateRepObject(msg *coormsg.PreUpdateRepObject) (*coormsg.PreUpdateRepObjectResp, *mq.CodeMessage) {
	// TODO 检查用户是否有Object的权限
	// 获取对象信息
	obj, err := svc.db.Object().GetByID(svc.db.SQLCtx(), msg.ObjectID)
	if err != nil {
		logger.WithField("ObjectID", msg.ObjectID).
			Warnf("get object failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OperationFailed, "get object failed")
	}
	if obj.Redundancy != models.RedundancyRep {
		logger.WithField("ObjectID", msg.ObjectID).
			Warnf("this object is not a rep object")
		return mq.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OperationFailed, "this object is not a rep object")
	}

	// 获取对象Rep信息
	objRep, err := svc.db.ObjectRep().GetByID(svc.db.SQLCtx(), msg.ObjectID)
	if err != nil {
		logger.WithField("ObjectID", msg.ObjectID).
			Warnf("get object rep failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OperationFailed, "get object rep failed")
	}

	//查询用户可用的节点IP
	nodes, err := svc.db.Node().GetUserNodes(svc.db.SQLCtx(), msg.UserID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warnf("query user nodes failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OperationFailed, "query user nodes failed")
	}

	// 查询客户端所属节点
	foundBelongNode := true
	belongNode, err := svc.db.Node().GetByExternalIP(svc.db.SQLCtx(), msg.ClientExternalIP)
	if err == sql.ErrNoRows {
		foundBelongNode = false
	} else if err != nil {
		logger.WithField("ClientExternalIP", msg.ClientExternalIP).
			Warnf("query client belong node failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OperationFailed, "query client belong node failed")
	}

	// 查询保存了旧文件的节点信息
	cachingNodes, err := svc.db.Cache().FindCachingFileUserNodes(svc.db.SQLCtx(), msg.UserID, objRep.FileHash)
	if err != nil {
		logger.Warnf("find caching file user nodes failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OperationFailed, "find caching file user nodes failed")
	}

	var retNodes []coormsg.PreUpdateRepObjectRespNode
	for _, node := range nodes {
		retNodes = append(retNodes, coormsg.NewPreUpdateRepObjectRespNode(
			node.NodeID,
			node.ExternalIP,
			node.LocalIP,
			// LocationID 相同则认为是在同一个地域
			foundBelongNode && belongNode.LocationID == node.LocationID,
			// 此节点存储了对象旧文件
			lo.ContainsBy(cachingNodes, func(n model.Node) bool { return n.NodeID == node.NodeID }),
		))
	}

	return mq.ReplyOK(coormsg.NewPreUpdateRepObjectResp(retNodes))
}

func (svc *Service) UpdateRepObject(msg *coormsg.UpdateRepObject) (*coormsg.UpdateRepObjectResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		return svc.db.Object().UpdateRepObject(tx, msg.ObjectID, msg.FileSize, msg.NodeIDs, msg.FileHash)
	})
	if err != nil {
		logger.WithField("ObjectID", msg.ObjectID).
			Warnf("update rep object failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.UpdateRepObjectResp](errorcode.OperationFailed, "update rep object failed")
	}

	// 紧急任务
	err = svc.scanner.PostEvent(scevt.NewCheckRepCount([]string{msg.FileHash}), true, true)
	if err != nil {
		logger.Warnf("post event to scanner failed, but this will not affect updating, err: %s", err.Error())
	}

	return mq.ReplyOK(coormsg.NewUpdateRepObjectResp())
}

func (svc *Service) DeleteObject(msg *coormsg.DeleteObject) (*coormsg.DeleteObjectResp, *mq.CodeMessage) {
	isAva, err := svc.db.Object().IsAvailable(svc.db.SQLCtx(), msg.UserID, msg.ObjectID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("ObjectID", msg.ObjectID).
			Warnf("check object available failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.DeleteObjectResp](errorcode.OperationFailed, "check object available failed")
	}
	if !isAva {
		logger.WithField("UserID", msg.UserID).
			WithField("ObjectID", msg.ObjectID).
			Warnf("object is not available to the user")
		return mq.ReplyFailed[coormsg.DeleteObjectResp](errorcode.OperationFailed, "object is not available to the user")
	}

	err = svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		return svc.db.Object().SoftDelete(tx, msg.ObjectID)
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("ObjectID", msg.ObjectID).
			Warnf("set object deleted failed, err: %s", err.Error())
		return mq.ReplyFailed[coormsg.DeleteObjectResp](errorcode.OperationFailed, "set object deleted failed")
	}

	stgs, err := svc.db.StorageObject().FindObjectStorages(svc.db.SQLCtx(), msg.ObjectID)
	if err != nil {
		logger.Warnf("find object storages failed, but this will not affect the deleting, err: %s", err.Error())
		return mq.ReplyOK(coormsg.NewDeleteObjectResp())
	}

	// 不追求及时、准确
	if len(stgs) == 0 {
		// 如果没有被引用，直接投递CheckObject的任务
		err := svc.scanner.PostEvent(scevt.NewCheckObject([]int64{msg.ObjectID}), false, false)
		if err != nil {
			logger.Warnf("post event to scanner failed, but this will not affect deleting, err: %s", err.Error())
		}
		logger.Debugf("post check object event")

	} else {
		// 有引用则让Agent去检查StorageObject
		for _, stg := range stgs {
			err := svc.scanner.PostEvent(scevt.NewAgentCheckStorage(stg.StorageID, []int64{msg.ObjectID}), false, false)
			if err != nil {
				logger.Warnf("post event to scanner failed, but this will not affect deleting, err: %s", err.Error())
			}
		}
		logger.Debugf("post agent check storage event")
	}

	return mq.ReplyOK(coormsg.NewDeleteObjectResp())
}
