package main

import (
	"fmt"
	"os"

	_ "google.golang.org/grpc/balancer/grpclb"

	"gitlink.org.cn/cloudream/client/config"

	"strconv"
)

func main() {
	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}

	args := os.Args
	switch args[1] {
	case "read":
		objectID, err := strconv.Atoi(args[3])
		if err != nil {
			fmt.Printf("invalid object id %s, err: %s", args[3], err.Error())
			os.Exit(1)
		}

		if err := Read(args[2], objectID); err != nil {
			fmt.Printf("read failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "write":
		bucketID, err := strconv.Atoi(args[3])
		if err != nil {
			fmt.Printf("invalid bucket id %s, err: %s", args[3], err.Error())
			os.Exit(1)
		}
		numRep, _ := strconv.Atoi(args[5])
		if numRep <= 0 || numRep > config.Cfg().MaxReplicateNumber {
			fmt.Printf("replicate number should not be more than %d", config.Cfg().MaxReplicateNumber)
			os.Exit(1)
		}

		if err := RepWrite(args[2], bucketID, args[4], numRep); err != nil {
			fmt.Printf("rep write failed, err: %s", err.Error())
			os.Exit(1)
		}
	case "ecWrite":
		bucketID, err := strconv.Atoi(args[3])
		if err != nil {
			fmt.Printf("invalid bucket id %s, err: %s", args[3], err.Error())
			os.Exit(1)
		}
		if err := EcWrite(args[2], bucketID, args[4], args[5]); err != nil {
			fmt.Printf("ec write failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "move":
		objectID, err := strconv.Atoi(args[2])
		if err != nil {
			fmt.Printf("invalid object id %s, err: %s", args[2], err.Error())
			os.Exit(1)
		}
		stgID, err := strconv.Atoi(args[3])
		if err != nil {
			fmt.Printf("invalid storage id %s, err: %s", args[3], err.Error())
			os.Exit(1)
		}

		if err := Move(objectID, stgID); err != nil {
			fmt.Printf("move failed, err: %s", err.Error())
			os.Exit(1)
		}
	}
	/*
		TO DO future:
		1. ls命令，显示用户指定桶下的所有对象，及相关的元数据
		2. rm命令，用户指定bucket和object名，执行删除操作
		3. update命令，用户发起对象更新命令，查询元数据，判断对象的冗余方式，删除旧对象（unpin所有的副本或编码块），写入新对象
		4. ipfsStat命令，查看本地有无ipfsdaemon，ipfs目录的使用率
		5. ipfsFlush命令，unpin本地ipfs目录中的所有cid(block)
		6. 改为交互式client，输入用户名及秘钥后进入交互界面
		7. 支持纯缓存类型的IPFS节点，数据一律存在后端存储服务中
	*/
}
