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
	//agentcaller "proto"
	//"github.com/pborman/uuid"
	//"github.com/streadway/amqp"
	//"google.golang.org/grpc"
)

func rabbitSend(c []byte, userId int) {
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
	redundancy := "rep"
	ecName := "ecName"
	hashs := []string{"block1.json", "block2.json"}
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
	rabbitSend(c, command.UserId)
}

func CoorEcWrite(command rabbitmq.EcWriteCommand) {
	fmt.Println("CoorEcWrite")
	fmt.Println(command.BucketName)
	//jh：根据command中的UserId查询用户节点权限表，返回用户可用的NodeIp
	//kx：根据command中的ecName，得到ecN，然后从jh查到的NodeIp中选择ecN个，赋值给Ips
	//jh：完成对象表、对象编码块表的插入（对象编码块表的Hash字段先不插入）
	//返回消息
	res := rabbitmq.WriteRes{
		Ips: []string{"localhost", "localhost", "localhost"},
	}
	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)

}

func CoorEcWriteHash(command rabbitmq.WriteHashCommand) {
	fmt.Println("CoorEcWriteHash")
	fmt.Println(command.BucketName)
	//jh：根据command中的信息，插入对象编码块表中的Hash字段，并完成缓存表的插入
	//返回消息
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
	res := rabbitmq.ReadRes{
		Redundancy:      "rep",
		Ips:             []string{"localhost", "localhost"},
		Hashs:           []string{"block1.json", "block2.json"},
		BlockIds:        []int{0, 1},
		EcName:          "ecName",
		FileSizeInBytes: 21,
	}
	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)
}

func CoorRepWriteHash(command rabbitmq.WriteHashCommand) {
	fmt.Println("CoorRepWriteHash")
	fmt.Println(command.BucketName)
	//jh：根据command中的信息，插入对象副本表中的Hash字段，并完成缓存表的插入
	//返回消息
	res := rabbitmq.WriteHashRes{
		MetaCode: 0,
	}
	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)
}

func CoorRepWrite(command rabbitmq.RepWriteCommand) {
	fmt.Println("CoorRepWrite")
	fmt.Println(command.BucketName)
	//jh：根据command中的UserId查询用户节点权限表，返回用户可用的NodeIp；
	//kx：根据command中的ecName，得到ecN，然后从jh查到的NodeIp中选择numRep个，赋值给Ips
	//jh：完成对象表、对象副本表的插入（对象副本表的Hash字段先不插入）
	//返回消息
	res := rabbitmq.WriteRes{
		Ips: []string{"localhost", "localhost", "localhost"},
	}
	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)
}

func HeartReport(command rabbitmq.HeartReport) {
	//jh：根据command中的Ip，插入节点延迟表，和节点表的NodeStatus
}
