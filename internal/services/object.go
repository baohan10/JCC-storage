package services

import (
	"database/sql"
	"errors"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db/model"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

func (svc *Service) PreDownloadObject(msg *coormsg.PreDownloadObject) *coormsg.PreDownloadObjectResp {

	// 查询文件对象
	object, err := svc.db.Object().GetUserObject(msg.Body.UserID, msg.Body.ObjectID)
	if err != nil {
		logger.WithField("ObjectID", msg.Body.ObjectID).
			Warnf("query Object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OPERATION_FAILED, "query Object failed")
	}

	// 查询客户端所属节点
	belongNode, err := svc.db.Node().GetByExternalIP(msg.Body.ClientExternalIP)
	if err != nil {
		logger.WithField("ClientExternalIP", msg.Body.ClientExternalIP).
			Warnf("query client belong node failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OPERATION_FAILED, "query client belong node failed")
	}
	logger.Debugf("client address %s is at location %d", msg.Body.ClientExternalIP, belongNode.LocationID)

	//-若redundancy是rep，查询对象副本表, 获得FileHash
	if object.Redundancy == consts.REDUNDANCY_REP {
		objectRep, err := svc.db.ObjectRep().GetObjectRep(object.ObjectID)
		if err != nil {
			logger.WithField("ObjectID", object.ObjectID).
				Warnf("get ObjectRep failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OPERATION_FAILED, "query ObjectRep failed")
		}

		// 注：由于采用了IPFS存储，因此每个备份文件的FileHash都是一样的
		nodes, err := svc.db.Cache().FindCachingFileUserNodes(svc.db.SQLCtx(), msg.Body.UserID, objectRep.FileHash)
		if err != nil {
			logger.WithField("FileHash", objectRep.FileHash).
				Warnf("query Cache failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OPERATION_FAILED, "query Cache failed")
		}

		var respNodes []ramsg.RespNode
		for _, node := range nodes {
			respNodes = append(respNodes, ramsg.NewRespNode(
				node.NodeID,
				node.ExternalIP,
				node.LocalIP,
				// LocationID 相同则认为是在同一个地域
				belongNode.LocationID == node.LocationID,
			))
		}

		return ramsg.ReplyOK(coormsg.NewPreDownloadObjectRespBody(
			object.Redundancy,
			object.FileSize,
			ramsg.NewRespObjectRepInfo(objectRep.FileHash, respNodes),
		))

	} else {
		// TODO 参考上面进行重写
		/*blocks, err := svc.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			log.WithField("ObjectID", object.ObjectID).
				Warnf("query Object Block failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.ReadResp](errorcode.OPERATION_FAILED, "query Object Block failed")
		}

		ecPolicies := *utils.GetEcPolicy()
		ecPolicy := ecPolicies[*object.ECName]
		ecN := ecPolicy.GetN()
		ecK := ecPolicy.GetK()
		nodeIPs = make([]string, ecN)
		hashes = make([]string, ecN)

		for _, tt := range blocks {
			id := tt.InnerID
			hash := tt.BlockHash
			hashes[id] = hash //这里有问题，采取的其实是直接顺序读的方式，等待加入自适应读模块

			nodes, err := svc.db.QueryCacheNodeByBlockHash(hash)
			if err != nil {
				log.WithField("BlockHash", hash).
					Warnf("query Cache failed, err: %s", err.Error())
				return ramsg.ReplyFailed[coormsg.ReadResp](errorcode.OPERATION_FAILED, "query Cache failed")
			}

			if len(nodes) == 0 {
				log.WithField("BlockHash", hash).
					Warnf("No node cache the block data for the BlockHash")
				return ramsg.ReplyFailed[coormsg.ReadResp](errorcode.OPERATION_FAILED, "No node cache the block data for the BlockHash")
			}

			nodeIPs[id] = nodes[0].IP
		}
		//这里也有和上面一样的问题
		for i := 1; i < ecK; i++ {
			blockIDs = append(blockIDs, i)
		}*/

		return ramsg.ReplyFailed[coormsg.PreDownloadObjectResp](errorcode.OPERATION_FAILED, "not implement yet!")
	}
}

func (svc *Service) PreUploadRepObject(msg *coormsg.PreUploadRepObject) *coormsg.PreUploadResp {

	// 判断同名对象是否存在。等到UploadRepObject时再判断一次。
	// 此次的判断只作为参考，具体是否成功还是看UploadRepObject的结果
	isBucketAvai, err := svc.db.Bucket().IsAvailable(msg.Body.BucketID, msg.Body.UserID)
	if err != nil {
		logger.WithField("BucketID", msg.Body.BucketID).
			Warnf("check bucket available failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreUploadResp](errorcode.OPERATION_FAILED, "check bucket available failed")
	}
	if !isBucketAvai {
		logger.WithField("BucketID", msg.Body.BucketID).
			Warnf("bucket is not available to user")
		return ramsg.ReplyFailed[coormsg.PreUploadResp](errorcode.OPERATION_FAILED, "bucket is not available to user")
	}

	_, err = svc.db.Object().GetByName(msg.Body.BucketID, msg.Body.ObjectName)
	if err == nil {
		logger.WithField("BucketID", msg.Body.BucketID).
			WithField("ObjectName", msg.Body.ObjectName).
			Warnf("object with given Name and BucketID already exists")
		return ramsg.ReplyFailed[coormsg.PreUploadResp](errorcode.OPERATION_FAILED, "object with given Name and BucketID already exists")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		logger.WithField("BucketID", msg.Body.BucketID).
			WithField("ObjectName", msg.Body.ObjectName).
			Warnf("get object by name failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreUploadResp](errorcode.OPERATION_FAILED, "get object by name failed")
	}

	//查询用户可用的节点IP
	nodes, err := svc.db.Node().GetUserNodes(msg.Body.UserID)
	if err != nil {
		logger.WithField("UserID", msg.Body.UserID).
			Warnf("query user nodes failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreUploadResp](errorcode.OPERATION_FAILED, "query user nodes failed")
	}

	// 查询客户端所属节点
	belongNode, err := svc.db.Node().GetByExternalIP(msg.Body.ClientExternalIP)
	if err != nil {
		logger.WithField("ClientExternalIP", msg.Body.ClientExternalIP).
			Warnf("query client belong node failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreUploadResp](errorcode.OPERATION_FAILED, "query client belong node failed")
	}

	var respNodes []ramsg.RespNode
	for _, node := range nodes {
		respNodes = append(respNodes, ramsg.NewRespNode(
			node.NodeID,
			node.ExternalIP,
			node.LocalIP,
			// LocationID 相同则认为是在同一个地域
			belongNode.LocationID == node.LocationID,
		))
	}

	return ramsg.ReplyOK(coormsg.NewPreUploadRespBody(respNodes))
}

func (svc *Service) CreateRepObject(msg *coormsg.CreateRepObject) *coormsg.CreateObjectResp {
	_, err := svc.db.Object().CreateRepObject(msg.Body.BucketID, msg.Body.ObjectName, msg.Body.FileSize, msg.Body.RepCount, msg.Body.NodeIDs, msg.Body.FileHash)
	if err != nil {
		logger.WithField("BucketName", msg.Body.BucketID).
			WithField("ObjectName", msg.Body.ObjectName).
			Warnf("create rep object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.CreateObjectResp](errorcode.OPERATION_FAILED, "create rep object failed")
	}

	// 紧急任务
	err = svc.scanner.PostEvent(scevt.NewCheckRepCount([]string{msg.Body.FileHash}), true, true)
	if err != nil {
		logger.Warnf("post event to scanner failed, but this will not affect creating, err: %s", err.Error())
	}

	return ramsg.ReplyOK(coormsg.NewCreateObjectRespBody())
}

func (svc *Service) PreUpdateRepObject(msg *coormsg.PreUpdateRepObject) *coormsg.PreUpdateRepObjectResp {
	// TODO 检查用户是否有Object的权限
	// 获取对象信息
	obj, err := svc.db.Object().GetByID(msg.Body.ObjectID)
	if err != nil {
		logger.WithField("ObjectID", msg.Body.ObjectID).
			Warnf("get object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OPERATION_FAILED, "get object failed")
	}
	if obj.Redundancy != consts.REDUNDANCY_REP {
		logger.WithField("ObjectID", msg.Body.ObjectID).
			Warnf("this object is not a rep object")
		return ramsg.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OPERATION_FAILED, "this object is not a rep object")
	}

	// 获取对象Rep信息
	objRep, err := svc.db.ObjectRep().GetObjectRep(msg.Body.ObjectID)
	if err != nil {
		logger.WithField("ObjectID", msg.Body.ObjectID).
			Warnf("get object rep failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OPERATION_FAILED, "get object rep failed")
	}

	//查询用户可用的节点IP
	nodes, err := svc.db.Node().GetUserNodes(msg.Body.UserID)
	if err != nil {
		logger.WithField("UserID", msg.Body.UserID).
			Warnf("query user nodes failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OPERATION_FAILED, "query user nodes failed")
	}

	// 查询客户端所属节点
	belongNode, err := svc.db.Node().GetByExternalIP(msg.Body.ClientExternalIP)
	if err != nil {
		logger.WithField("ClientExternalIP", msg.Body.ClientExternalIP).
			Warnf("query client belong node failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OPERATION_FAILED, "query client belong node failed")
	}

	// 查询保存了旧文件的节点信息
	cachingNodes, err := svc.db.Cache().FindCachingFileUserNodes(svc.db.SQLCtx(), msg.Body.UserID, objRep.FileHash)
	if err != nil {
		logger.Warnf("find caching file user nodes failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreUpdateRepObjectResp](errorcode.OPERATION_FAILED, "find caching file user nodes failed")
	}

	var retNodes []coormsg.PreUpdateRepObjectRespNode
	for _, node := range nodes {
		retNodes = append(retNodes, coormsg.NewPreUpdateRepObjectRespNode(
			node.NodeID,
			node.ExternalIP,
			node.LocalIP,
			// LocationID 相同则认为是在同一个地域
			belongNode.LocationID == node.LocationID,
			// 此节点存储了对象旧文件
			lo.ContainsBy(cachingNodes, func(n model.Node) bool { return n.NodeID == node.NodeID }),
		))
	}

	return ramsg.ReplyOK(coormsg.NewPreUpdateRepObjectRespBody(retNodes))
}

func (svc *Service) UpdateRepObject(msg *coormsg.UpdateRepObject) *coormsg.UpdateRepObjectResp {
	err := svc.db.Object().UpdateRepObject(msg.Body.ObjectID, msg.Body.FileSize, msg.Body.NodeIDs, msg.Body.FileHash)
	if err != nil {
		logger.WithField("ObjectID", msg.Body.ObjectID).
			Warnf("update rep object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.UpdateRepObjectResp](errorcode.OPERATION_FAILED, "update rep object failed")
	}

	// 紧急任务
	err = svc.scanner.PostEvent(scevt.NewCheckRepCount([]string{msg.Body.FileHash}), true, true)
	if err != nil {
		logger.Warnf("post event to scanner failed, but this will not affect updating, err: %s", err.Error())
	}

	return ramsg.ReplyOK(coormsg.NewUpdateRepObjectRespBody())
}

func (svc *Service) DeleteObject(msg *coormsg.DeleteObject) *coormsg.DeleteObjectResp {
	err := svc.db.Object().SoftDelete(msg.Body.UserID, msg.Body.ObjectID)

	if err != nil {
		logger.WithField("UserID", msg.Body.UserID).
			WithField("ObjectID", msg.Body.ObjectID).
			Warnf("set object deleted failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.DeleteObjectResp](errorcode.OPERATION_FAILED, "set object deleted failed")
	}

	stgs, err := svc.db.StorageObject().FindObjectStorages(svc.db.SQLCtx(), msg.Body.ObjectID)
	if err != nil {
		logger.Warnf("find object storages failed, but this will not affect the deleting, err: %s", err.Error())
		return ramsg.ReplyOK(coormsg.NewDeleteObjectRespBody())
	}

	// 不追求及时、准确
	if len(stgs) == 0 {
		// 如果没有被引用，直接投递CheckObject的任务
		err := svc.scanner.PostEvent(scevt.NewCheckObject([]int{msg.Body.ObjectID}), false, false)
		if err != nil {
			logger.Warnf("post event to scanner failed, but this will not affect deleting, err: %s", err.Error())
		}
		logger.Debugf("post check object event")

	} else {
		// 有引用则让Agent去检查StorageObject
		for _, stg := range stgs {
			err := svc.scanner.PostEvent(scevt.NewAgentCheckStorage(stg.StorageID, []int{msg.Body.ObjectID}), false, false)
			if err != nil {
				logger.Warnf("post event to scanner failed, but this will not affect deleting, err: %s", err.Error())
			}
		}
		logger.Debugf("post agent check storage event")
	}

	return ramsg.ReplyOK(coormsg.NewDeleteObjectRespBody())
}
