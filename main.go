package main

import (

	//"path/filepath"
	//"sync"

	"time"
	//agentcaller "proto"
	//"github.com/pborman/uuid"
	//"github.com/streadway/amqp"
	//"google.golang.org/grpc"
)

func main() {
	for {
		//jh:遍历对象副本表，
		//-对于每一个rephash,
		//--根据objectId查询对象表中的RepNum
		//--查询缓存表中rephash对应的TempOrPin为false的nodeIp
		//--如果查到的NodeIp数少于RepNum，需发起复制命令
		//jh:遍历对象编码块表，
		//-对于每一个blockhash,获得其blockId、objectId、innerID
		//--判断blockhash是否在ipfs网络中
		//--得到待修复object清单：记录下各个objectId对应的不在ipfs网络中的blockId、blockhash、innerID
		//-查询待修复object清单中各个object的FileSize、EcName等，并发出修复命令
		time.Sleep(time.Minute * 5)
	}
}
