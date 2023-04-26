package services

import (
	log "github.com/sirupsen/logrus"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils"
	"gitlink.org.cn/cloudream/utils/consts"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
)

func (service *Service) Read(msg *coormsg.ReadCommand) *coormsg.ReadResp {
	var hashes []string
	blockIDs := []int{0}

	// 查询文件对象
	object, err := service.db.QueryObjectByID(msg.ObjectID)
	if err != nil {
		log.WithField("ObjectID", msg.ObjectID).
			Warnf("query Object failed, err: %s", err.Error())
		return coormsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query Object failed")
	}

	var nodeIPs []string
	//-若redundancy是rep，查询对象副本表, 获得repHash
	if object.Redundancy == consts.REDUNDANCY_REP {
		objectRep, err := service.db.QueryObjectRep(object.ObjectID)
		if err != nil {
			log.WithField("ObjectID", object.ObjectID).
				Warnf("query ObjectRep failed, err: %s", err.Error())
			return coormsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query ObjectRep failed")
		}

		hashes = append(hashes, objectRep.RepHash)

		nodes, err := service.db.QueryCacheNodeByBlockHash(objectRep.RepHash)
		if err != nil {
			log.WithField("RepHash", objectRep.RepHash).
				Warnf("query Cache failed, err: %s", err.Error())
			return coormsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query Cache failed")
		}

		for _, node := range nodes {
			nodeIPs = append(nodeIPs, node.IP)
		}

	} else {
		blocks, err := service.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			log.WithField("ObjectID", object.ObjectID).
				Warnf("query Object Block failed, err: %s", err.Error())
			return coormsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query Object Block failed")
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

			nodes, err := service.db.QueryCacheNodeByBlockHash(hash)
			if err != nil {
				log.WithField("BlockHash", hash).
					Warnf("query Cache failed, err: %s", err.Error())
				return coormsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query Cache failed")
			}

			if len(nodes) == 0 {
				log.WithField("BlockHash", hash).
					Warnf("No node cache the block data for the BlockHash")
				return coormsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "No node cache the block data for the BlockHash")
			}

			nodeIPs[id] = nodes[0].IP
		}
		//这里也有和上面一样的问题
		for i := 1; i < ecK; i++ {
			blockIDs = append(blockIDs, i)
		}
	}

	return coormsg.NewCoorReadRespOK(
		object.Redundancy,
		nodeIPs,
		hashes,
		blockIDs,
		object.ECName,
		object.FileSizeInBytes,
	)
}

func (service *Service) RepWrite(msg *coormsg.RepWriteCommand) *coormsg.WriteResp {
	// TODO 需要在此处判断同名对象是否存在。等到WriteRepHash时再判断一次。
	// 此次的判断只作为参考，具体是否成功还是看WriteRepHash的结果

	//查询用户可用的节点IP
	nodes, err := service.db.QueryUserNodes(msg.UserID)
	if err != nil {
		log.Warnf("query user nodes failed, err: %s", err.Error())
		return coormsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, "query user nodes failed")
	}

	if len(nodes) < msg.ReplicateNumber {
		log.WithField("UserID", msg.UserID).
			WithField("ReplicateNumber", msg.ReplicateNumber).
			Warnf("user nodes are not enough")
		return coormsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, "user nodes are not enough")
	}

	numRep := msg.ReplicateNumber
	ids := make([]int, numRep)
	ips := make([]string, numRep)
	//随机选取numRep个nodeIp
	start := utils.GetRandInt(len(nodes))
	for i := 0; i < numRep; i++ {
		index := (start + i) % len(nodes)
		ids[i] = nodes[index].NodeID
		ips[i] = nodes[index].IP
	}

	return coormsg.NewCoorWriteRespOK(ids, ips)
}

func (service *Service) WriteRepHash(msg *coormsg.WriteRepHashCommand) *coormsg.WriteHashResp {
	_, err := service.db.CreateRepObject(msg.BucketID, msg.ObjectName, msg.FileSizeInBytes, msg.ReplicateNumber, msg.NodeIDs, msg.Hashes)
	if err != nil {
		log.WithField("BucketName", msg.BucketID).
			WithField("ObjectName", msg.ObjectName).
			Warnf("create rep object failed, err: %s", err.Error())
		return coormsg.NewCoorWriteHashRespFailed(errorcode.OPERATION_FAILED, "create rep object failed")
	}

	return coormsg.NewCoorWriteHashRespOK()
}

func (svc *Service) DeleteObject(msg *coormsg.DeleteObject) *coormsg.DeleteObjectResp {
	err := svc.db.SetObjectDeleted(msg.UserID, msg.ObjectID)

	if err != nil {
		log.WithField("UserID", msg.UserID).
			WithField("ObjectID", msg.ObjectID).
			Warnf("set object deleted failed, err: %s", err.Error())
		return coormsg.NewDeleteObjectRespFailed(errorcode.OPERATION_FAILED, "set object deleted failed")
	}

	return coormsg.NewDeleteObjectRespOK()
}
