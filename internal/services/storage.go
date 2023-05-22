package services

import (
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/utils"
	log "gitlink.org.cn/cloudream/common/utils/logger"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

func (service *Service) PreMoveObjectToStorage(msg *coormsg.PreMoveObjectToStorage) *coormsg.PreMoveObjectToStorageResp {
	//查询数据库，获取冗余类型，冗余参数
	//jh:使用command中的bucketname和objectname查询对象表,获得redundancy，EcName,fileSize
	//-若redundancy是rep，查询对象副本表, 获得repHash
	//--ids ：={0}
	//--hashs := {repHash}
	//-若redundancy是ec,查询对象编码块表，获得blockHashs, ids(innerID),
	//--查询缓存表，获得每个hash的nodeIps、TempOrPins、Times
	//--查询节点延迟表，得到command.destination与各个nodeIps的的延迟，存到一个map类型中（Delay）
	//--kx:根据查出来的hash/hashs、nodeIps、TempOrPins、Times(移动/读取策略)、Delay确定hashs、ids

	// 查询用户关联的存储服务
	stg, err := service.db.Storage().GetUserStorage(msg.Body.UserID, msg.Body.StorageID)
	if err != nil {
		log.WithField("UserID", msg.Body.UserID).
			WithField("StorageID", msg.Body.StorageID).
			Warnf("get user Storage failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OPERATION_FAILED, "get user Storage failed")
	}

	// 查询文件对象
	object, err := service.db.Object().GetUserObject(msg.Body.UserID, msg.Body.ObjectID)
	if err != nil {
		log.WithField("ObjectID", msg.Body.ObjectID).
			Warnf("get user Object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OPERATION_FAILED, "get user Object failed")
	}

	//-若redundancy是rep，查询对象副本表, 获得FileHash
	if object.Redundancy == consts.REDUNDANCY_REP {
		objectRep, err := service.db.ObjectRep().GetObjectRep(object.ObjectID)
		if err != nil {
			log.Warnf("get ObjectRep failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OPERATION_FAILED, "get ObjectRep failed")
		}

		return ramsg.ReplyOK(coormsg.NewPreMoveObjectToStorageRespBody(
			stg.NodeID,
			stg.Directory,
			object.FileSize,
			object.Redundancy,
			ramsg.NewObjectRepInfo(objectRep.FileHash),
		))

	} else {
		// TODO 以EC_开头的Redundancy才是EC策略

		var hashs []string
		ids := []int{0}
		blockHashs, err := service.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			log.Warnf("query ObjectBlock failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OPERATION_FAILED, "query ObjectBlock failed")
		}

		ecPolicies := *utils.GetEcPolicy()
		ecPolicy := ecPolicies[object.Redundancy]
		ecN := ecPolicy.GetN()
		ecK := ecPolicy.GetK()
		ids = make([]int, ecK)
		for i := 0; i < ecN; i++ {
			hashs = append(hashs, "-1")
		}
		for i := 0; i < ecK; i++ {
			ids[i] = i
		}
		hashs = make([]string, ecN)
		for _, tt := range blockHashs {
			id := tt.InnerID
			hash := tt.BlockHash
			hashs[id] = hash
		}
		//--查询缓存表，获得每个hash的nodeIps、TempOrPins、Times
		/*for id,hash := range blockHashs{
			//type Cache struct {NodeIP string,TempOrPin bool,Cachetime string}
			Cache := Query_Cache(hash)
			//利用Time_trans()函数可将Cache[i].Cachetime转化为时间戳格式
			//--查询节点延迟表，得到command.Destination与各个nodeIps的延迟，存到一个map类型中（Delay）
			Delay := make(map[string]int) // 延迟集合
			for i:=0; i<len(Cache); i++{
				Delay[Cache[i].NodeIP] = Query_NodeDelay(Destination, Cache[i].NodeIP)
			}
			//--kx:根据查出来的hash/hashs、nodeIps、TempOrPins、Times(移动/读取策略)、Delay确定hashs、ids
		}*/
		return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OPERATION_FAILED, "not implement yet!")
	}
}

func (service *Service) MoveObjectToStorage(msg *coormsg.MoveObjectToStorage) *coormsg.MoveObjectToStorageResp {
	err := service.db.Storage().UserMoveObjectTo(msg.Body.UserID, msg.Body.ObjectID, msg.Body.StorageID)
	if err != nil {
		log.WithField("UserID", msg.Body.UserID).
			WithField("ObjectID", msg.Body.ObjectID).
			WithField("StorageID", msg.Body.StorageID).
			Warnf("user move object to storage failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.MoveObjectToStorageResp](errorcode.OPERATION_FAILED, "user move object to storage failed")
	}

	return ramsg.ReplyOK(coormsg.NewMoveObjectToStorageRespBody())
}
