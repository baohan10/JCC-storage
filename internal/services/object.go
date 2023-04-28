package services

import (
	log "github.com/sirupsen/logrus"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils"
	"gitlink.org.cn/cloudream/utils/consts"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
)

func (svc *Service) Read(msg *coormsg.ReadCommand) *coormsg.ReadResp {

	// 查询文件对象
	object, err := svc.db.QueryObjectByID(msg.Body.ObjectID)
	if err != nil {
		log.WithField("ObjectID", msg.Body.ObjectID).
			Warnf("query Object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.ReadResp](errorcode.OPERATION_FAILED, "query Object failed")
	}

	// 查询客户端所属节点
	belongNode, err := svc.db.FindNodeByExternalIP(msg.Body.ReaderExternalIP)
	if err != nil {
		log.WithField("ReaderExternalIP", msg.Body.ReaderExternalIP).
			Warnf("query client belong node failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.ReadResp](errorcode.OPERATION_FAILED, "query client belong node failed")
	}
	log.Debugf("client address %s is at location %d", msg.Body.ReaderExternalIP, belongNode.LocationID)

	var entries []coormsg.ReadRespEntry
	//-若redundancy是rep，查询对象副本表, 获得repHash
	if object.Redundancy == consts.REDUNDANCY_REP {
		objectReps, err := svc.db.GetObjectReps(object.ObjectID)
		if err != nil {
			log.WithField("ObjectID", object.ObjectID).
				Warnf("get ObjectRep failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.ReadResp](errorcode.OPERATION_FAILED, "query ObjectRep failed")
		}

		if len(objectReps) == 0 {
			log.WithField("ObjectID", object.ObjectID).
				Warnf("object rep not found")
			return ramsg.ReplyFailed[coormsg.ReadResp](errorcode.OPERATION_FAILED, "object rep not found")
		}

		// 由于采用了IPFS存储，因此每个备份文件的FileHash都是一样的，此处直接使用第一个
		nodes, err := svc.db.QueryCacheNodeByBlockHash(objectReps[0].RepHash)
		if err != nil {
			log.WithField("RepHash", objectReps[0].RepHash).
				Warnf("query Cache failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.ReadResp](errorcode.OPERATION_FAILED, "query Cache failed")
		}

		for _, node := range nodes {
			entries = append(entries, coormsg.NewReadRespEntry(
				node.NodeID,
				node.ExternalIP,
				node.LocalIP,
				// LocationID 相同则认为是在同一个地域
				belongNode.LocationID == node.LocationID,
				objectReps[0].RepHash,
				0))
		}

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
	}

	return ramsg.ReplyOK(coormsg.NewReadRespBody(
		object.Redundancy,
		object.ECName,
		object.FileSizeInBytes,
		entries,
	))
}

func (svc *Service) RepWrite(msg *coormsg.RepWriteCommand) *coormsg.WriteResp {
	// TODO 需要在此处判断同名对象是否存在。等到WriteRepHash时再判断一次。
	// 此次的判断只作为参考，具体是否成功还是看WriteRepHash的结果

	//查询用户可用的节点IP
	nodes, err := svc.db.QueryUserNodes(msg.Body.UserID)
	if err != nil {
		log.Warnf("query user nodes failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.WriteResp](errorcode.OPERATION_FAILED, "query user nodes failed")
	}

	if len(nodes) < msg.Body.ReplicateNumber {
		log.WithField("UserID", msg.Body.UserID).
			WithField("ReplicateNumber", msg.Body.ReplicateNumber).
			Warnf("user nodes are not enough")
		return ramsg.ReplyFailed[coormsg.WriteResp](errorcode.OPERATION_FAILED, "user nodes are not enough")
	}

	// 查询客户端所属节点
	belongNode, err := svc.db.FindNodeByExternalIP(msg.Body.WriterExternalIP)
	if err != nil {
		log.WithField("WriterExternalIP", msg.Body.WriterExternalIP).
			Warnf("query client belong node failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.WriteResp](errorcode.OPERATION_FAILED, "query client belong node failed")
	}

	numRep := msg.Body.ReplicateNumber
	retNodes := make([]coormsg.WriteRespNode, numRep)
	//随机选取numRep个nodeIp
	start := utils.GetRandInt(len(nodes))
	for i := 0; i < numRep; i++ {
		index := (start + i) % len(nodes)
		node := nodes[index]

		retNodes[i] = coormsg.NewWriteRespNode(node.NodeID, node.ExternalIP, node.LocalIP, belongNode.LocationID == node.LocationID)
	}

	return ramsg.ReplyOK(coormsg.NewCoorWriteRespBody(retNodes))
}

func (service *Service) WriteRepHash(msg *coormsg.WriteRepHashCommand) *coormsg.WriteHashResp {
	_, err := service.db.CreateRepObject(msg.Body.BucketID, msg.Body.ObjectName, msg.Body.FileSizeInBytes, msg.Body.ReplicateNumber, msg.Body.NodeIDs, msg.Body.Hashes)
	if err != nil {
		log.WithField("BucketName", msg.Body.BucketID).
			WithField("ObjectName", msg.Body.ObjectName).
			Warnf("create rep object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.WriteHashResp](errorcode.OPERATION_FAILED, "create rep object failed")
	}

	return ramsg.ReplyOK(coormsg.NewWriteHashRespBody())
}

func (svc *Service) DeleteObject(msg *coormsg.DeleteObject) *coormsg.DeleteObjectResp {
	err := svc.db.SetObjectDeleted(msg.Body.UserID, msg.Body.ObjectID)

	if err != nil {
		log.WithField("UserID", msg.Body.UserID).
			WithField("ObjectID", msg.Body.ObjectID).
			Warnf("set object deleted failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.DeleteObjectResp](errorcode.OPERATION_FAILED, "set object deleted failed")
	}

	return ramsg.ReplyOK(coormsg.NewDeleteObjectRespBody())
}
