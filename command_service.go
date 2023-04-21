package main

import (
	log "github.com/sirupsen/logrus"
	mydb "gitlink.org.cn/cloudream/db"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	"gitlink.org.cn/cloudream/utils"

	"gitlink.org.cn/cloudream/utils/consts"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
)

type CommandService struct {
	db *mydb.DB
}

func NewCommandService(db *mydb.DB) *CommandService {
	return &CommandService{
		db: db,
	}
}

func (service *CommandService) Read(msg *ramsg.ReadCommand) ramsg.ReadResp {
	var hashes []string
	blockIDs := []int{0}

	// 查询文件对象
	object, err := service.db.QueryObjectByID(msg.BucketID, msg.ObjectID)
	if err != nil {
		log.WithField("BucketID", msg.BucketID).
			WithField("ObjectID", msg.ObjectID).
			Warnf("query Object failed, err: %s", err.Error())
		return ramsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query Object failed")
	}

	var nodeIPs []string
	//-若redundancy是rep，查询对象副本表, 获得repHash
	if object.Redundancy == consts.REDUNDANCY_REP {
		objectRep, err := service.db.QueryObjectRep(object.ObjectID)
		if err != nil {
			log.WithField("ObjectID", object.ObjectID).
				Warnf("query ObjectRep failed, err: %s", err.Error())
			return ramsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query ObjectRep failed")
		}

		hashes = append(hashes, objectRep.RepHash)

		nodes, err := service.db.QueryCacheNodeByBlockHash(objectRep.RepHash)
		if err != nil {
			log.WithField("RepHash", objectRep.RepHash).
				Warnf("query Cache failed, err: %s", err.Error())
			return ramsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query Cache failed")
		}

		for _, node := range nodes {
			nodeIPs = append(nodeIPs, node.IP)
		}

	} else {
		blocks, err := service.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			log.WithField("ObjectID", object.ObjectID).
				Warnf("query Object Block failed, err: %s", err.Error())
			return ramsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query Object Block failed")
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
				return ramsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "query Cache failed")
			}

			if len(nodes) == 0 {
				log.WithField("BlockHash", hash).
					Warnf("No node cache the block data for the BlockHash")
				return ramsg.NewCoorReadRespFailed(errorcode.OPERATION_FAILED, "No node cache the block data for the BlockHash")
			}

			nodeIPs[id] = nodes[0].IP
		}
		//这里也有和上面一样的问题
		for i := 1; i < ecK; i++ {
			blockIDs = append(blockIDs, i)
		}
	}

	return ramsg.NewCoorReadRespOK(
		object.Redundancy,
		nodeIPs,
		hashes,
		blockIDs,
		object.ECName,
		object.FileSizeInBytes,
	)
}

func (service *CommandService) RepWrite(msg *ramsg.RepWriteCommand) ramsg.WriteResp {
	// TODO 需要在此处判断同名对象是否存在。等到WriteRepHash时再判断一次。
	// 此次的判断只作为参考，具体是否成功还是看WriteRepHash的结果

	//查询用户可用的节点IP
	nodes, err := service.db.QueryUserNodes(msg.UserID)
	if err != nil {
		log.Warnf("query user nodes failed, err: %s", err.Error())
		return ramsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, "query user nodes failed")
	}

	if len(nodes) < msg.ReplicateNumber {
		log.WithField("UserID", msg.UserID).
			WithField("ReplicateNumber", msg.ReplicateNumber).
			Warnf("user nodes are not enough")
		return ramsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, "user nodes are not enough")
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

	return ramsg.NewCoorWriteRespOK(ids, ips)
}

func (service *CommandService) WriteRepHash(msg *ramsg.WriteRepHashCommand) ramsg.WriteHashResp {
	_, err := service.db.CreateRepObject(msg.BucketID, msg.ObjectName, msg.FileSizeInBytes, msg.ReplicateNumber, msg.NodeIDs, msg.Hashes)
	if err != nil {
		log.WithField("BucketName", msg.BucketID).
			WithField("ObjectName", msg.ObjectName).
			Warnf("create rep object failed, err: %s", err.Error())
		return ramsg.NewCoorWriteHashRespFailed(errorcode.OPERATION_FAILED, "create rep object failed")
	}

	return ramsg.NewCoorWriteHashRespOK()
}

func (service *CommandService) Move(msg *ramsg.MoveCommand) ramsg.MoveResp {
	//查询数据库，获取冗余类型，冗余参数
	//jh:使用command中的bucketname和objectname查询对象表,获得redundancy，EcName,fileSizeInBytes
	//-若redundancy是rep，查询对象副本表, 获得repHash
	//--ids ：={0}
	//--hashs := {repHash}
	//-若redundancy是ec,查询对象编码块表，获得blockHashs, ids(innerID),
	//--查询缓存表，获得每个hash的nodeIps、TempOrPins、Times
	//--查询节点延迟表，得到command.destination与各个nodeIps的的延迟，存到一个map类型中（Delay）
	//--kx:根据查出来的hash/hashs、nodeIps、TempOrPins、Times(移动/读取策略)、Delay确定hashs、ids

	// 查询用户关联的存储服务
	stg, err := service.db.QueryUserStorage(msg.UserID, msg.StorageID)
	if err != nil {
		log.WithField("UserID", msg.UserID).
			WithField("StorageID", msg.StorageID).
			Warnf("query storage directory failed, err: %s", err.Error())
		return ramsg.NewCoorMoveRespFailed(errorcode.OPERATION_FAILED, "query storage directory failed")
	}

	// 查询文件对象
	object, err := service.db.QueryObjectByFullName(msg.BucketName, msg.ObjectName)
	if err != nil {
		log.WithField("BucketName", msg.BucketName).
			WithField("ObjectName", msg.ObjectName).
			Warnf("query Object failed, err: %s", err.Error())
		return ramsg.NewCoorMoveRespFailed(errorcode.OPERATION_FAILED, "query Object failed")
	}

	//-若redundancy是rep，查询对象副本表, 获得repHash
	var hashs []string
	ids := []int{0}
	if object.Redundancy == consts.REDUNDANCY_REP {
		objectRep, err := service.db.QueryObjectRep(object.ObjectID)
		if err != nil {
			log.Warnf("query ObjectRep failed, err: %s", err.Error())
			return ramsg.NewCoorMoveRespFailed(errorcode.OPERATION_FAILED, "query ObjectRep failed")
		}

		hashs = append(hashs, objectRep.RepHash)

	} else {
		blockHashs, err := service.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			log.Warnf("query ObjectBlock failed, err: %s", err.Error())
			return ramsg.NewCoorMoveRespFailed(errorcode.OPERATION_FAILED, "query ObjectBlock failed")
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

	return ramsg.NewCoorMoveRespOK(
		stg.NodeID,
		stg.Directory,
		object.Redundancy,
		object.ECName,
		hashs,
		ids,
		object.FileSizeInBytes,
	)
}

func (service *CommandService) TempCacheReport(msg *ramsg.TempCacheReport) {
	service.db.BatchInsertOrUpdateCache(msg.Hashes, msg.NodeID)
}

func (service *CommandService) AgentStatusReport(msg *ramsg.AgentStatusReport) {
	//jh：根据command中的Ip，插入节点延迟表，和节点表的NodeStatus
	//根据command中的Ip，插入节点延迟表

	// TODO
	/*
		ips := utils.GetAgentIps()
		Insert_NodeDelay(msg.IP, ips, msg.AgentDelay)

		//从配置表里读取节点地域NodeLocation
		//插入节点表的NodeStatus
		Insert_Node(msg.IP, msg.IP, msg.IPFSStatus, msg.LocalDirStatus)
	*/
}
