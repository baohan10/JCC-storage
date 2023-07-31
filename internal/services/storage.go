package services

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/common/utils"
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
			object.FileSize,
			models.NewRedundancyRepData(objectRep.FileHash),
		))

	} else {
		// TODO 以EC_开头的Redundancy才是EC策略

		var hashs []string
		ids := []int{0}
		blockHashs, err := svc.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			logger.Warnf("query ObjectBlock failed, err: %s", err.Error())
			return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OperationFailed, "query ObjectBlock failed")
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
		return ramsg.ReplyFailed[coormsg.PreMoveObjectToStorageResp](errorcode.OperationFailed, "not implement yet!")
	}
}

func (svc *Service) MoveObjectToStorage(msg *coormsg.MoveObjectToStorage) (*coormsg.MoveObjectToStorageResp, *ramsg.CodeMessage) {
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
