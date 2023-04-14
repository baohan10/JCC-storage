package main

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	mydb "gitlink.org.cn/cloudream/db"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	"gitlink.org.cn/cloudream/utils"
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

// TODO 需要考虑数据库操作的事务性
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
	bucketID, err := service.db.QueryBucketID(msg.BucketName)
	if err != nil {
		log.Warn("query BucketID failed, err: %s", err.Error())
		return ramsg.NewCoorMoveRespFailed(errorcode.OPERATION_FAILED, "query BucketID failed")
	}

	//jh:使用command中的bucketid和objectname查询对象表,获得objectid,redundancy，EcName,fileSizeInBytes
	object, err := service.db.QueryObject(msg.ObjectName, bucketID)
	if err != nil {
		log.Warn("query Object failed, err: %s", err.Error())
		return ramsg.NewCoorMoveRespFailed(errorcode.OPERATION_FAILED, "query Object failed")
	}

	//-若redundancy是rep，查询对象副本表, 获得repHash
	var hashs []string
	ids := []int{0}
	redundancy := "rep"
	if object.Redundancy { //rep
		objectRep, err := service.db.QueryObjectRep(object.ObjectID)
		if err != nil {
			log.Warn("query ObjectRep failed, err: %s", err.Error())
			return ramsg.NewCoorMoveRespFailed(errorcode.OPERATION_FAILED, "query ObjectRep failed")
		}

		hashs = append(hashs, objectRep.RepHash)

	} else { //ec
		redundancy = "ec"
		blockHashs, err := service.db.QueryObjectBlock(object.ObjectID)
		if err != nil {
			log.Warn("query ObjectBlock failed, err: %s", err.Error())
			return ramsg.NewCoorMoveRespFailed(errorcode.OPERATION_FAILED, "query ObjectBlock failed")
		}

		ecPolicies := *utils.GetEcPolicy()
		ecPolicy := ecPolicies[object.ECName]
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
		redundancy,
		object.ECName,
		hashs,
		ids,
		object.FileSizeInBytes,
	)
}

func (service *CommandService) RepWrite(msg *ramsg.RepWriteCommand) ramsg.WriteResp {
	//查询用户可用的节点IP
	nodeIPs, err := service.db.QueryUserAvailableNodeIPs(msg.UserID)
	if err != nil {
		log.Warn("query user available node ip failed, err: %s", err.Error())
		return ramsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, "query user available node ip failed")
	}

	if len(nodeIPs) < msg.ReplicateNumber {
		log.Warn("user nodes are not enough, err: %s", err.Error())
		return ramsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, "user nodes are not enough")
	}

	numRep := msg.ReplicateNumber
	ips := make([]string, numRep)
	//随机选取numRep个nodeIp
	start := utils.GetRandInt(len(nodeIPs))
	for i := 0; i < numRep; i++ {
		ips[i] = nodeIPs[(start+i)%len(nodeIPs)]
	}

	_, err = service.db.CreateRepObject(msg.ObjectName, msg.BucketName, msg.FileSizeInBytes, msg.ReplicateNumber)
	if err != nil {
		log.Warn("create object failed, err: %s", err.Error())
		return ramsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, "create object failed")
	}

	return ramsg.NewCoorWriteRespOK(ips)
}

func (service *CommandService) WriteRepHash(msg *ramsg.WriteRepHashCommand) ramsg.WriteHashResp {
	//jh：根据command中的信息，插入对象副本表中的Hash字段，并完成缓存表的插入
	//插入对象副本表中的Hash字段

	//TODO xh: objectID的查询合并到Insert_RepHash函数中去
	ObjectId := Query_ObjectID(msg.ObjectName)
	Insert_RepHash(ObjectId, msg.Hashes[0])

	//缓存表的插入
	Insert_Cache(msg.Hashes, msg.IPs, false)

	//返回消息
	return ramsg.NewCoorWriteHashRespOK()
}

func (service *CommandService) ECWrite(msg *ramsg.ECWriteCommand) ramsg.WriteResp {
	//jh：根据command中的UserId查询用户节点权限表，返回用户可用的NodeIp
	//kx：根据command中的ecName，得到ecN，然后从jh查到的NodeIp中选择ecN个，赋值给Ips
	//jh：完成对象表、对象编码块表的插入（对象编码块表的Hash字段先不插入）
	//返回消息
	nodeip := Query_UserNode(msg.UserID) //nodeip格式：[]string
	ecid := msg.ECName
	ecPolicies := *utils.GetEcPolicy()
	ecPolicy := ecPolicies[ecid]
	ecN := ecPolicy.GetN()

	ips := make([]string, ecN)
	//kx：从jh查到的NodeIp中选择ecN个，赋值给Ips
	//根据BucketName查询BucketID
	start := utils.GetRandInt(len(nodeip))
	for i := 0; i < ecN; i++ {
		ips[i] = nodeip[(start+i)%len(nodeip)]
	}

	//根据BucketName查询BucketID
	BucketID := Query_BucketID(msg.BucketName)
	if BucketID == -1 {
		// TODO 日志
		return ramsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, fmt.Sprintf("bucket id not found for %s", msg.BucketName))
	}
	//对象表插入Insert_Cache
	ObjectID := Insert_EcObject(msg.ObjectName, BucketID, msg.FileSizeInBytes, msg.ECName)
	//对象编码块表插入，hash暂时为空
	for i := 0; i < ecN; i++ {
		Insert_EcObjectBlock(ObjectID, i)
	}
	return ramsg.NewCoorWriteRespOK(ips)
}

func (service *CommandService) WriteECHash(msg *ramsg.WriteECHashCommand) ramsg.WriteHashResp {
	//jh：根据command中的信息，插入对象编码块表中的Hash字段，并完成缓存表的插入
	//返回消息
	//插入对象编码块表中的Hash字段
	ObjectId := Query_ObjectID(msg.ObjectName)
	Insert_EcHash(ObjectId, msg.Hashes)
	//缓存表的插入
	Insert_Cache(msg.Hashes, msg.IPs, false)

	return ramsg.NewCoorWriteHashRespOK()
}

func (service *CommandService) Read(msg *ramsg.ReadCommand) ramsg.ReadResp {
	var ips, hashs []string
	blockIds := []int{0}
	//先查询
	BucketID := Query_BucketID(msg.BucketName)
	//jh:使用command中的bucketid和objectname查询对象表,获得objectid,redundancy，EcName,fileSizeInBytes
	/*
		TODO xh:
		redundancyy（bool型）这个变量名不规范（应该是为了与redundancy（字符型）分开而随意取的名），需调整：
			只用redundancy变量，且将其类型调整为bool（用常量EC表示false,REP表示true），ReadRes结构体的定义做相应修改
	*/
	ObjectID, fileSizeInBytes, redundancyy, ecName := Query_Object(msg.ObjectName, BucketID)
	//-若redundancy是rep，查询对象副本表, 获得repHash
	redundancy := "rep"
	if redundancyy { //rep
		repHash := Query_ObjectRep(ObjectID)
		hashs[0] = repHash
		caches := Query_Cache(repHash)
		//TODO xh: 所有错误消息均不可吃掉，记录到日志里
		for _, cache := range caches {
			ip := cache.NodeIP
			ips = append(ips, ip)
		}

	} else { //ec
		redundancy = "ec"
		blockHashs := Query_ObjectBlock(ObjectID)
		ecPolicies := *utils.GetEcPolicy()
		ecPolicy := ecPolicies[ecName]
		ecN := ecPolicy.GetN()
		ecK := ecPolicy.GetK()
		for i := 0; i < ecN; i++ {
			ips = append(ips, "-1")
			hashs = append(hashs, "-1")
		}
		for _, tt := range blockHashs {
			id := tt.InnerID
			hash := tt.BlockHash
			hashs[id] = hash //这里有问题，采取的其实是直接顺序读的方式，等待加入自适应读模块
			cache := Query_Cache(hash)
			ip := cache[0].NodeIP
			ips[id] = ip
		}
		//这里也有和上面一样的问题
		for i := 1; i < ecK; i++ {
			blockIds = append(blockIds, i)
		}
	}

	return ramsg.NewCoorReadRespOK(
		redundancy,
		ips,
		hashs,
		blockIds,
		ecName,
		fileSizeInBytes,
	)
}

func (service *CommandService) TempCacheReport(msg *ramsg.TempCacheReport) {
	service.db.BatchInsertOrUpdateCache(msg.Hashes, msg.IP)
}

func (service *CommandService) AgentStatusReport(msg *ramsg.AgentStatusReport) {
	//jh：根据command中的Ip，插入节点延迟表，和节点表的NodeStatus
	//根据command中的Ip，插入节点延迟表
	ips := utils.GetAgentIps()
	Insert_NodeDelay(msg.IP, ips, msg.AgentDelay)

	//从配置表里读取节点地域NodeLocation
	//插入节点表的NodeStatus
	Insert_Node(msg.IP, msg.IP, msg.IPFSStatus, msg.LocalDirStatus)
}
