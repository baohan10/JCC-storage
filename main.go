package main

import (
	//"context"
	//"io"
	"fmt"
	"os"

	//"path/filepath"
	//"sync"
	"strconv"
	//agentcaller "proto"

	//"github.com/pborman/uuid"
	//"github.com/streadway/amqp"

	//"google.golang.org/grpc"

	_ "google.golang.org/grpc/balancer/grpclb"
)

func main() {
	//TODO xh:加载配置文件，获取packetSizeInBytes、grpc port、ipfs port、最大副本数、本机公网Ip等信息，参照src/utils/config.go

	args := os.Args
	arg_num := len(os.Args)
	for i := 0; i < arg_num; i++ {
		fmt.Println(args[i])
	}
	//TODO: 改为交互式client，输入用户名及秘钥后进入交互界面
	switch args[1] {
	case "ecWrite":
		EcWrite(args[2], args[3], args[4], args[5])
		//TODO: 写入对象时，Coor判断对象是否已存在，如果存在，则直接返回
	case "write":
		numRep, _ := strconv.Atoi(args[5])
		if numRep <= 0 || numRep > 10 { //TODO xh:10改为从配置文件中读出的最大副本数
			print("write::InputError!") //TODO xh:优化提示语
		} else {
			RepWrite(args[2], args[3], args[4], numRep)
		}
	case "read":
		Read(args[2], args[3], args[4])
	case "move":
		Move(args[2], args[3], args[4]) //bucket object destination
	}
	/*
		TO DO:
		1. ls命令，显示用户指定桶下的所有对象，及相关的元数据
		2. rm命令，用户指定bucket和object名，执行删除操作
		3. update命令，用户发起对象更新命令，查询元数据，判断对象的冗余方式，删除旧对象（unpin所有的副本或编码块），写入新对象
		4. ipfsStat命令，查看本地有无ipfsdaemon，ipfs目录的使用率
		5. ipfsFlush命令，unpin本地ipfs目录中的所有cid(block)
	*/
}
