package main

import (
	//"context"
	//"io"
	"fmt"
	//"path/filepath"
	//"sync"
	"encoding/json"
	"rabbitmq"
	"strconv"
	"utils"
	//agentcaller "proto"
	//"github.com/pborman/uuid"
	//"github.com/streadway/amqp"
	//"google.golang.org/grpc"
)

func rabbitSend(c []byte, userId int) {
	//TO DO xh： queueName调整：coorClientQueue+"_"+"本机Ip"+"_"+"进程号"
	queueName := "coorClientQueue" + strconv.Itoa(userId)
	rabbit := rabbitmq.NewRabbitMQSimple(queueName)
	fmt.Println(string(c))
	rabbit.PublishSimple(c)
	rabbit.Destroy()
}

func TempCacheReport(command rabbitmq.TempCacheReport) {
	fmt.Println("TempCacheReport")
	fmt.Println(command.Hashs)
	fmt.Println(command.Ip)
	//返回消息
	for i := 0; i < len(command.Hashs); i++ {
		cache := Update_Cache(command.Hashs[i], command.Ip)
		//若要插入缓存不存在，将hashs中的hash，IP插入缓存表中，TempOrPin字段为true，Time为插入时的时间戳
		if cache == (Cache{}) {
			a := []string{}
			b := []string{}
			a = append(a, command.Hashs[i])
			b = append(b, command.Ip)
			Insert_Cache(a, b, true)
		}
	}
	//jh:将hashs中的hash，IP插入缓存表中，TempOrPin字段为true，Time为插入时的时间戳
	//-如果要插入的hash、IP在表中已存在且所对应的TempOrPin字段为false，则不做任何操作
	//-如果要插入的hash、IP在表中已存在且所对应的TempOrPin字段为true，则更新Time
}

func CoorMove(command rabbitmq.MoveCommand) {
	fmt.Println("CoorMove")
	fmt.Println(command.BucketName)
	//查询数据库，获取冗余类型，冗余参数
	//jh:使用command中的bucketname和objectname查询对象表,获得redundancy，EcName,fileSizeInBytes
	//-若redundancy是rep，查询对象副本表, 获得repHash
	//--ids ：={0}
	//--hashs := {repHash}
	//-若redundancy是ec,查询对象编码块表，获得blockHashs, ids(innerID),
	//--查询缓存表，获得每个hash的nodeIps、TempOrPins、Times
	//--查询节点延迟表，得到command.destination与各个nodeIps的的延迟，存到一个map类型中（Delay）
	//--kx:根据查出来的hash/hashs、nodeIps、TempOrPins、Times(移动/读取策略)、Delay确定hashs、ids
	BucketID := Query_BucketID(command.BucketName)
	//jh:使用command中的bucketid和objectname查询对象表,获得objectid,redundancy，EcName,fileSizeInBytes
	ObjectID, fileSizeInBytes, redundancyy, ecName := Query_Object(command.ObjectName, BucketID)
	//-若redundancy是rep，查询对象副本表, 获得repHash

	var hashs []string
	ids := []int{0}
	redundancy := "rep"
	if redundancyy { //rep
		repHash := Query_ObjectRep(ObjectID)
		hashs = append(hashs, repHash)
	} else { //ec
		redundancy = "ec"
		blockHashs := Query_ObjectBlock(ObjectID)
		ecPolicies := *utils.GetEcPolicy()
		ecPolicy := ecPolicies[ecName]
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
	//--ids ：={0}
	//--hashs := {repHash}

	//-若redundancy是ec,查询对象编码块表，获得blockHashs, ids(innerID)
	// var objectblock []ObjectBlock
	//type ObjectBlock struct {InnerID int, BlockHash string}
	/*redundancy := "rep"
	ecName := "ecName"
	hashs := []string{"QmPeaWD8vrtnd1CE4WLnKBtYnMmAeVmUhqzhMnJ3QvLHsZ", "block2.json"}
	ids := []int{0, 1}
	fileSizeInBytes := 21

	res := rabbitmq.MoveRes{
		Redundancy:      redundancy,
		EcName:          ecName,
		Hashs:           hashs,
		Ids:             ids,
		FileSizeInBytes: int64(fileSizeInBytes),
	}
	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)*/
	/*ec Move测试
	redundancy := "ec"
	ecName := "rs_3_2"
	hashs := []string{"QmZD9YwEXpYAw5zncYzYv7LN9K6bw29FcbYsdL4whiRAyz", "QmSzxPyMJfw8fgwWHmEEMBpcdg8vqHtQuRAjUbzbv3KrEa"}
	ids := []int{1, 2}
	fileSizeInBytes := 21*/

	res := rabbitmq.MoveRes{
		Redundancy:      redundancy,
		EcName:          ecName,
		Hashs:           hashs,
		Ids:             ids,
		FileSizeInBytes: int64(fileSizeInBytes),
	}
	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)

}

func CoorEcWrite(command rabbitmq.EcWriteCommand) {
	fmt.Println("CoorEcWrite")
	fmt.Println(command.BucketName)
	//jh：根据command中的UserId查询用户节点权限表，返回用户可用的NodeIp
	//kx：根据command中的ecName，得到ecN，然后从jh查到的NodeIp中选择ecN个，赋值给Ips
	//jh：完成对象表、对象编码块表的插入（对象编码块表的Hash字段先不插入）
	//返回消息
	nodeip := Query_UserNode(command.UserId) //nodeip格式：[]string
	ecid := command.EcName
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
	BucketID := Query_BucketID(command.BucketName)
	if BucketID == -1 { //bucket不对
		res := rabbitmq.WriteRes{
			Ips: nil, //先让客户端崩溃
		}
		c, _ := json.Marshal(res)
		rabbitSend(c, command.UserId)
		return
	}
	//对象表插入Insert_Cache
	ObjectID := Insert_EcObject(command.ObjectName, BucketID, command.FileSizeInBytes, command.EcName)
	//对象编码块表插入，hash暂时为空
	for i := 0; i < ecN; i++ {
		Insert_EcObjectBlock(ObjectID, i)
	}

	res := rabbitmq.WriteRes{
		Ips: ips,
	}
	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)

}

func CoorEcWriteHash(command rabbitmq.WriteHashCommand) {
	fmt.Println("CoorEcWriteHash")
	fmt.Println(command.BucketName)
	//jh：根据command中的信息，插入对象编码块表中的Hash字段，并完成缓存表的插入
	//返回消息
	//插入对象编码块表中的Hash字段
	ObjectId := Query_ObjectID(command.ObjectName)
	Insert_EcHash(ObjectId, command.Hashs)
	//缓存表的插入
	Insert_Cache(command.Hashs, command.Ips, false)
	res := rabbitmq.WriteHashRes{
		MetaCode: 0,
	}
	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)
}

func CoorRead(command rabbitmq.ReadCommand) {
	fmt.Println("CoorRead")
	fmt.Println(command.BucketName)
	//jh:使用command中的bucketname和objectname查询对象表,获得redundancy，EcName,fileSizeInBytes
	//-若redundancy是rep，查询对象副本表, 获得repHash
	//--ids ：={0}
	//--hashs := {repHash}
	//-若redundancy是ec,查询对象编码块表，获得blockHashs, ids(innerID),
	//--查询缓存表，获得每个hash的nodeIps、TempOrPins、Times
	//--kx:根据查出来的hash/hashs、nodeIps、TempOrPins、Times(移动/读取策略)确定hashs、ids
	//返回消息

	/*res := rabbitmq.ReadRes{
		Redundancy:      "rep",
		Ips:             []string{"localhost", "localhost"},
		Hashs:           []string{"block1.json", "block2.json"},
		BlockIds:        []int{0, 1},
		EcName:          "ecName",
		FileSizeInBytes: 21,
	}*/

	var ips, hashs []string
	blockIds := []int{0}
	//先查询
	BucketID := Query_BucketID(command.BucketName)
	//jh:使用command中的bucketid和objectname查询对象表,获得objectid,redundancy，EcName,fileSizeInBytes
	ObjectID, fileSizeInBytes, redundancyy, ecName := Query_Object(command.ObjectName, BucketID)
	//-若redundancy是rep，查询对象副本表, 获得repHash
	redundancy := "rep"
	if redundancyy { //rep
		repHash := Query_ObjectRep(ObjectID)
		hashs[0] = repHash
		caches := Query_Cache(repHash)
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
	/*
		res := rabbitmq.ReadRes{
			Redundancy:      "ec",
			Ips:             ips,
			Hashs:           []string{"QmZD9YwEXpYAw5zncYzYv7LN9K6bw29FcbYsdL4whiRAyz", "QmSzxPyMJfw8fgwWHmEEMBpcdg8vqHtQuRAjUbzbv3KrEa"},
			BlockIds:        []int{1, 2},
			EcName:          "ecName",
			FileSizeInBytes: 21,
		}*/

	res := rabbitmq.ReadRes{
		Redundancy:      redundancy,
		Ips:             ips,
		Hashs:           hashs,
		BlockIds:        blockIds,
		EcName:          ecName,
		FileSizeInBytes: fileSizeInBytes,
	}

	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)
}

func CoorRepWriteHash(command rabbitmq.WriteHashCommand) {
	fmt.Println("CoorRepWriteHash")
	fmt.Println(command.BucketName)
	//jh：根据command中的信息，插入对象副本表中的Hash字段，并完成缓存表的插入
	//插入对象副本表中的Hash字段
	//TODO xh: objectID的查询合并到Insert_RepHash函数中去
	ObjectId := Query_ObjectID(command.ObjectName)
	Insert_RepHash(ObjectId, command.Hashs[0])
	//缓存表的插入
	Insert_Cache(command.Hashs, command.Ips, false)
	//返回消息
	res := rabbitmq.WriteHashRes{
		MetaCode: 0,
	}
	c, _ := json.Marshal(res)
	//TODO xh: 传入client的ip和pid
	rabbitSend(c, command.UserId)
}

func CoorRepWrite(command rabbitmq.RepWriteCommand) {
	fmt.Println("CoorRepWrite")
	fmt.Println(command.BucketName)
	//查询用户可用的节点IP
	nodeip := Query_UserNode(command.UserId) //nodeip格式：[]string
	numRep := command.NumRep
	/*
		TODO xh:错误判断
			1.判断用户可用节点是否小于numRep
			2.判断用户要写入的对象是否已存在
			如果存在上述错误，直接通过消息队列返回错误代码给客户端（需在rabbitmq/commands.go中给writeRes加上错误代码字段）
	*/
	ips := make([]string, numRep)
	//随机选取numRep个nodeIp
	start := utils.GetRandInt(len(nodeip))
	for i := 0; i < numRep; i++ {
		ips[i] = nodeip[(start+i)%len(nodeip)]
	}
	//TODO xh: Query_BucketID和Insert_RepObject合成一个命令(两个动作可以一条sql语句搞定)，保留Query_BucketID函数，
	//但这里不使用它，而是把command.BucketName也传入Insert_RepObject，让其完成查询和插入操作，
	//TODO xh: 元数据插入失败，需返回错误码
	BucketID := Query_BucketID(command.BucketName)
	//对象表插入
	ObjectID := Insert_RepObject(command.ObjectName, BucketID, command.FileSizeInBytes, command.NumRep)
	//对象副本表的插入
	Insert_ObjectRep(ObjectID)
	//返回消息
	res := rabbitmq.WriteRes{
		Ips: ips,
	}
	c, _ := json.Marshal(res)
	//TODO xh：传入clientIp和进程号
	rabbitSend(c, command.UserId)
}

func HeartReport(command rabbitmq.HeartReport) {
	//jh：根据command中的Ip，插入节点延迟表，和节点表的NodeStatus
	//根据command中的Ip，插入节点延迟表
	ips := utils.GetAgentIps()
	Insert_NodeDelay(command.Ip, ips, command.AgentDelay)

	//从配置表里读取节点地域NodeLocation
	//插入节点表的NodeStatus
	Insert_Node(command.Ip, command.Ip, command.IpfsStatus, command.LocalDirStatus)
}
