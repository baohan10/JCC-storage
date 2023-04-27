package services

import (
	log "github.com/sirupsen/logrus"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils"
	"gitlink.org.cn/cloudream/utils/consts"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
)

func (service *Service) Move(msg *coormsg.MoveObjectToStorage) *coormsg.MoveObjectToStorageResp {
	//查询数据库，获取冗余类型，冗余参数
	//jh:使用command中的bucketname和objectname查询对象表,获得redundancy，EcName,fileSizeInBytes
	//-若redundancy是rep，查询对象副本表, 获得repHash
	//--ids ：={0}
	//--hashs := {repHash}
	//-若redundancy是ec,查询对象编码块表，获得blockHashs, ids(innerID),
	//--查询缓存表，获得每个hash的nodeIps、TempOrPins、Times
	//--查询节点延迟表，得到command.destination与各个nodeIps的的延迟，存到一个map类型中（Delay）
	//--kx:根据查出来的hash/hashs、nodeIps、TempOrPins、Times(移动/读取策略)、Delay确定hashs、ids

	// TODO 需要在StorageData中增加记录

	// 查询用户关联的存储服务
	stg, err := service.db.QueryUserStorage(msg.Body.UserID, msg.Body.StorageID)
	if err != nil {
		log.WithField("UserID", msg.Body.UserID).
			WithField("StorageID", msg.Body.StorageID).
			Warnf("query storage directory failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.MoveObjectToStorageResp](errorcode.OPERATION_FAILED, "query storage directory failed")
	}

	// 查询文件对象
	object, err := service.db.QueryObjectByID(msg.Body.ObjectID)
	if err != nil {
		log.WithField("ObjectID", msg.Body.ObjectID).
			Warnf("query Object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[coormsg.MoveObjectToStorageResp](errorcode.OPERATION_FAILED, "query Object failed")
	}

	//-若redundancy是rep，查询对象副本表, 获得repHash
	var hashs []string
	ids := []int{0}
	if object.Redundancy == consts.REDUNDANCY_REP {
		objectRep, err := service.db.QueryObjectRep(object.ObjectID)
		if err != nil {
			log.Warnf("query ObjectRep failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.MoveObjectToStorageResp](errorcode.OPERATION_FAILED, "query ObjectRep failed")
		}

		hashs = append(hashs, objectRep.RepHash)

	} else {
		blockHashs, err := service.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			log.Warnf("query ObjectBlock failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.MoveObjectToStorageResp](errorcode.OPERATION_FAILED, "query ObjectBlock failed")
		}

		ecPolicies := *utils.GetEcPolicy()
		ecPolicy := ecPolicies[*object.ECName]
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
	}

	return ramsg.ReplyOK(coormsg.NewMoveObjectToStorageRespBody(
		stg.NodeID,
		stg.Directory,
		object.Redundancy,
		object.ECName,
		hashs,
		ids,
		object.FileSizeInBytes,
	))
}
