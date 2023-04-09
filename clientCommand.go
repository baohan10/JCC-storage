package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	agentcaller "proto"
	"rabbitmq"
	"strconv"
	"sync"
	"time"

	//"reflect"
	//"github.com/pborman/uuid"
	//"github.com/streadway/amqp"
	"ec"
	"utils"

	"google.golang.org/grpc"

	_ "google.golang.org/grpc/balancer/grpclb"
)

const (
	port              = ":5010"
	packetSizeInBytes = 10
)

//TODO xh:调整函数顺序，以及函数名大小写
func Move(bucketName string, objectName string, destination string) {
	//将bucketName, objectName, destination发给协调端
	fmt.Println("move " + bucketName + "/" + objectName + " to " + destination)
	//获取块hash，ip，序号，编码参数等
	//发送写请求，分配写入节点Ip
	userId := 0
	command1 := rabbitmq.MoveCommand{
		BucketName:  bucketName,
		ObjectName:  objectName,
		UserId:      userId,
		Destination: destination,
	}
	c1, _ := json.Marshal(command1)
	b1 := append([]byte("05"), c1...)
	fmt.Println(string(b1))
	rabbit1 := rabbitmq.NewRabbitMQSimple("coorQueue")
	rabbit1.PublishSimple(b1)

	//接收消息，赋值给ip, repHash, fileSizeInBytes
	var res1 rabbitmq.MoveRes
	var redundancy string
	var hashs []string
	var fileSizeInBytes int64
	var ecName string
	var ids []int
	queueName := "coorClientQueue" + strconv.Itoa(userId)
	rabbit2 := rabbitmq.NewRabbitMQSimple(queueName)
	msgs := rabbit2.ConsumeSimple(time.Millisecond, true)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for d := range msgs {
			_ = json.Unmarshal(d.Body, &res1)
			redundancy = res1.Redundancy
			ids = res1.Ids
			hashs = res1.Hashs
			fileSizeInBytes = res1.FileSizeInBytes
			ecName = res1.EcName
			wg.Done()
		}
	}()
	wg.Wait()
	fmt.Println(redundancy)
	fmt.Println(hashs)
	fmt.Println(ids)
	fmt.Println(fileSizeInBytes)
	fmt.Println(ecName)
	//根据redundancy调用repMove和ecMove

	rabbit3 := rabbitmq.NewRabbitMQSimple("agentQueue" + destination)
	var b2 []byte
	switch redundancy {
	case "rep":
		command2 := rabbitmq.RepMoveCommand{
			Hashs:           hashs,
			BucketName:      bucketName,
			ObjectName:      objectName,
			UserId:          userId,
			FileSizeInBytes: fileSizeInBytes,
		}
		c2, _ := json.Marshal(command2)
		b2 = append([]byte("00"), c2...)
	case "ec":
		command2 := rabbitmq.EcMoveCommand{
			Hashs:           hashs,
			Ids:             ids,
			EcName:          ecName,
			BucketName:      bucketName,
			ObjectName:      objectName,
			UserId:          userId,
			FileSizeInBytes: fileSizeInBytes,
		}
		c2, _ := json.Marshal(command2)
		b2 = append([]byte("01"), c2...)
	}
	fmt.Println(b2)
	rabbit3.PublishSimple(b2)
	//接受调度成功与否的消息
	//接受第二轮通讯结果
	var res2 rabbitmq.AgentMoveRes
	queueName = "agentClientQueue" + strconv.Itoa(userId)
	rabbit4 := rabbitmq.NewRabbitMQSimple(queueName)
	msgs = rabbit4.ConsumeSimple(time.Millisecond, true)
	wg.Add(1)
	go func() {
		for d := range msgs {
			_ = json.Unmarshal(d.Body, &res2)
			if res2.MoveCode == 0 {
				wg.Done()
				fmt.Println("Move Success")
			}
		}
	}()
	wg.Wait()

	rabbit1.Destroy()
	rabbit2.Destroy()
	rabbit3.Destroy()
	rabbit4.Destroy()
}

func Read(localFilePath string, bucketName string, objectName string) {
	fmt.Println("read " + bucketName + "/" + objectName + " to " + localFilePath)
	//获取块hash，ip，序号，编码参数等
	//发送写请求，分配写入节点Ip
	userId := 0
	command1 := rabbitmq.ReadCommand{
		BucketName: bucketName,
		ObjectName: objectName,
		UserId:     userId,
	}
	c1, _ := json.Marshal(command1)
	//TODO xh: 用常量定义"02"等
	b1 := append([]byte("02"), c1...)
	fmt.Println(b1)
	rabbit1 := rabbitmq.NewRabbitMQSimple("coorQueue")
	rabbit1.PublishSimple(b1)

	//接收消息，赋值给ip, repHash, fileSizeInBytes
	var res1 rabbitmq.ReadRes
	var hashs []string
	var ips []string
	var fileSizeInBytes int64
	var ecName string
	var ids []int
	var redundancy string
	queueName := "coorClientQueue" + strconv.Itoa(userId)
	rabbit2 := rabbitmq.NewRabbitMQSimple(queueName)
	msgs := rabbit2.ConsumeSimple(time.Millisecond, true)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		//TODO xh: 增加消息队列等待超时机制（配置文件指定最长等待时间，超时报错返回）
		for d := range msgs {
			_ = json.Unmarshal(d.Body, &res1)
			ips = res1.Ips
			hashs = res1.Hashs
			ids = res1.BlockIds
			ecName = res1.EcName
			fileSizeInBytes = res1.FileSizeInBytes
			redundancy = res1.Redundancy
			wg.Done()
		}
	}()
	wg.Wait()
	fmt.Println(redundancy)
	fmt.Println(ips)
	fmt.Println(hashs)
	fmt.Println(ids)
	fmt.Println(ecName)
	fmt.Println(fileSizeInBytes)
	rabbit1.Destroy()
	rabbit2.Destroy()
	switch redundancy {
	//TODO xh: redundancy换为bool型，用常量EC表示ec,REP表示rep
	case "rep":
		repRead(fileSizeInBytes, ips[0], hashs[0], localFilePath)
	case "ec":
		ecRead(fileSizeInBytes, ips, hashs, ids, ecName, localFilePath)
	}

}

func repRead(fileSizeInBytes int64, ip string, repHash string, localFilePath string) {
	numPacket := (fileSizeInBytes + packetSizeInBytes - 1) / (packetSizeInBytes)
	fmt.Println(numPacket)
	//rpc相关
	conn, err := grpc.Dial(ip+port, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := agentcaller.NewTranBlockOrReplicaClient(conn)

	fDir, err := os.Executable()
	if err != nil {
		panic(err)
	}
	//TODO xh:删除assets，改为读到当前目录与localFilePath的叠加路径
	fURL := filepath.Join(filepath.Dir(fDir), "assets")
	_, err = os.Stat(fURL)
	if os.IsNotExist(err) {
		os.MkdirAll(fURL, os.ModePerm)
	}

	file, err := os.Create(filepath.Join(fURL, localFilePath))
	if err != nil {
		return
	}
	/*
		TO DO: 判断本地有没有ipfs daemon、能否获取相应对象的cid
			如果本地有ipfs daemon且能获取相应对象的cid，则获取对象cid对应的ipfsblock的cid，通过ipfs网络获取这些ipfsblock
			否则，像目前一样，使用grpc向指定节点获取
	*/
	stream, _ := client.GetBlockOrReplica(context.Background(), &agentcaller.GetReq{
		BlockOrReplicaHash: repHash,
	})
	fmt.Println(numPacket)
	for i := 0; int64(i) < numPacket; i++ {
		fmt.Println(i)
		res, _ := stream.Recv()
		fmt.Println(res.BlockOrReplicaData)
		file.Write(res.BlockOrReplicaData)
	}
	file.Close()
	conn.Close()
}

func RepWrite(localFilePath string, bucketName string, objectName string, numRep int) {
	userId := 0
	//获取文件大小
	fileInfo, _ := os.Stat(localFilePath)
	fileSizeInBytes := fileInfo.Size()
	fmt.Println(fileSizeInBytes)

	//写入对象的packet数
	numWholePacket := fileSizeInBytes / packetSizeInBytes
	lastPacketInBytes := fileSizeInBytes % packetSizeInBytes
	numPacket := numWholePacket
	if lastPacketInBytes > 0 {
		numPacket++
	}

	//发送写请求，请求Coor分配写入节点Ip
	//TO DO： 加入两个字段，本机IP和当前进程号
	command1 := rabbitmq.RepWriteCommand{
		BucketName:      bucketName,
		ObjectName:      objectName,
		FileSizeInBytes: fileSizeInBytes,
		NumRep:          numRep,
		UserId:          userId,
	}
	c1, _ := json.Marshal(command1)
	b1 := append([]byte("03"), c1...)
	fmt.Println(b1)
	rabbit1 := rabbitmq.NewRabbitMQSimple("coorQueue")
	rabbit1.PublishSimple(b1)

	var res1 rabbitmq.WriteRes
	var ips []string
	/*
		TODO xh: 判断writeRes里的状态码
			如果有错，就报错返回，结束程序
			如果没错，就把得到的IP值赋给ips
	*/

	//TODO xh: queueName调整：coorClientQueue+"_"+"本机Ip"+"_"+"进程号"
	queueName := "coorClientQueue" + strconv.Itoa(userId)
	rabbit2 := rabbitmq.NewRabbitMQSimple(queueName)
	msgs := rabbit2.ConsumeSimple(time.Millisecond, true)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for d := range msgs {
			_ = json.Unmarshal(d.Body, &res1)
			ips = res1.Ips
		}
		wg.Done()
	}()
	wg.Wait()

	//创建channel
	loadDistributeBufs := make([]chan []byte, numRep)
	for i := 0; i < numRep; i++ {
		loadDistributeBufs[i] = make(chan []byte)
	}

	//正式开始写入
	hashs := make([]string, numRep)
	go loadDistribute(localFilePath, loadDistributeBufs[:], numWholePacket, lastPacketInBytes) //从本地文件系统加载数据
	wg.Add(numRep)
	for i := 0; i < numRep; i++ {
		//TODO xh: send的第一个参数不需要了
		go send("rep.json"+strconv.Itoa(i), ips[i], loadDistributeBufs[i], numPacket, &wg, hashs, i) //"block1.json"这样参数不需要
	}
	wg.Wait()

	//第二轮通讯:插入元数据hashs
	//TODO xh: 加入pid字段
	command2 := rabbitmq.WriteHashCommand{
		BucketName: bucketName,
		ObjectName: objectName,
		Hashs:      hashs,
		Ips:        ips,
		UserId:     userId,
	}
	c1, _ = json.Marshal(command2)
	b1 = append([]byte("04"), c1...)
	rabbit1.PublishSimple(b1)

	//接受第二轮通讯结果
	var res2 rabbitmq.WriteHashRes
	msgs = rabbit2.ConsumeSimple(time.Millisecond, true)
	wg.Add(1)
	go func() {
		for d := range msgs {
			_ = json.Unmarshal(d.Body, &res2)
			if res2.MetaCode == 0 {
				wg.Done()
			}
			//TODO xh: MetaCode不为零，代表插入出错，需输出错误
		}
	}()
	wg.Wait()
	rabbit1.Destroy()
	rabbit2.Destroy()
	//
}

func ecRead(fileSizeInBytes int64, ips []string, blockHashs []string, blockIds []int, ecName string, localFilePath string) {
	//根据ecName获得以下参数
	wg := sync.WaitGroup{}
	ecPolicies := *utils.GetEcPolicy()
	ecPolicy := ecPolicies[ecName]
	fmt.Println(ecPolicy)
	ecK := ecPolicy.GetK()
	ecN := ecPolicy.GetN()
	var coefs = [][]int64{{1, 1, 1}, {1, 2, 3}} //2应替换为ecK，3应替换为ecN

	numPacket := (fileSizeInBytes + int64(ecK)*packetSizeInBytes - 1) / (int64(ecK) * packetSizeInBytes)
	fmt.Println(numPacket)
	//创建channel
	getBufs := make([]chan []byte, ecN)
	decodeBufs := make([]chan []byte, ecK)
	for i := 0; i < ecN; i++ {
		getBufs[i] = make(chan []byte)
	}
	for i := 0; i < ecK; i++ {
		decodeBufs[i] = make(chan []byte)
	}
	//从协调端获取有哪些编码块
	//var blockSeq = []int{0,1}
	blockSeq := blockIds
	wg.Add(1)
	for i := 0; i < len(blockSeq); i++ {
		go get(blockHashs[i], ips[i], getBufs[blockSeq[i]], numPacket)
	}
	go decode(getBufs[:], decodeBufs[:], blockSeq, ecK, coefs, numPacket)
	go persist(decodeBufs[:], numPacket, localFilePath, &wg)
	wg.Wait()

}

func EcWrite(localFilePath string, bucketName string, objectName string, ecName string) {
	fmt.Println("write " + localFilePath + " as " + bucketName + "/" + objectName)
	//获取文件大小
	fileInfo, _ := os.Stat(localFilePath)
	fileSizeInBytes := fileInfo.Size()
	fmt.Println(fileSizeInBytes)
	//调用纠删码库，获取编码参数及生成矩阵
	ecPolicies := *utils.GetEcPolicy()
	ecPolicy := ecPolicies[ecName]

	ipss := utils.GetAgentIps()
	fmt.Println(ipss)
	print("@!@!@!@!@!@!")

	//var policy utils.EcConfig
	//policy = ecPolicy[0]
	ecK := ecPolicy.GetK()
	ecN := ecPolicy.GetN()
	//const ecK int = ecPolicy.GetK()
	//const ecN int = ecPolicy.GetN()
	var coefs = [][]int64{{1, 1, 1}, {1, 2, 3}} //2应替换为ecK，3应替换为ecN

	//计算每个块的packet数
	numPacket := (fileSizeInBytes + int64(ecK)*packetSizeInBytes - 1) / (int64(ecK) * packetSizeInBytes)
	fmt.Println(numPacket)

	//发送写请求，分配写入节点
	userId := 0
	//发送写请求，分配写入节点Ip
	command1 := rabbitmq.EcWriteCommand{
		BucketName:      bucketName,
		ObjectName:      objectName,
		FileSizeInBytes: fileSizeInBytes,
		EcName:          ecName,
		UserId:          userId,
	} //
	c1, _ := json.Marshal(command1)
	b1 := append([]byte("00"), c1...) //
	fmt.Println(b1)
	rabbit1 := rabbitmq.NewRabbitMQSimple("coorQueue")
	rabbit1.PublishSimple(b1)

	//接收消息，赋值给ips
	var res1 rabbitmq.WriteRes
	var ips []string
	queueName := "coorClientQueue" + strconv.Itoa(userId)
	rabbit2 := rabbitmq.NewRabbitMQSimple(queueName)
	msgs := rabbit2.ConsumeSimple(time.Millisecond, true)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for d := range msgs {
			_ = json.Unmarshal(d.Body, &res1)
			ips = res1.Ips
			wg.Done()
		}
	}()
	wg.Wait()

	//创建channel
	loadBufs := make([]chan []byte, ecN)
	encodeBufs := make([]chan []byte, ecN)
	for i := 0; i < ecN; i++ {
		loadBufs[i] = make(chan []byte)
	}
	for i := 0; i < ecN; i++ {
		encodeBufs[i] = make(chan []byte)
	}
	blockNames := make([]string, ecN)
	for i := 0; i < ecN; i++ {
		blockNames[i] = (bucketName + "_" + objectName + "_" + strconv.Itoa(i))
		print(blockNames[i])
		print("miemiemie")
	}
	hashs := make([]string, ecN)
	//正式开始写入
	go load(localFilePath, loadBufs[:ecN], ecK, numPacket*int64(ecK), fileSizeInBytes) //从本地文件系统加载数据
	go encode(loadBufs[:ecN], encodeBufs[:ecN], ecK, coefs, numPacket)

	wg.Add(ecN)

	for i := 0; i < ecN; i++ {
		go send(blockNames[i], ips[i], encodeBufs[i], numPacket, &wg, hashs, i)
	}
	wg.Wait()
	//fmt.Println(hashs)
	//第二轮通讯:插入元数据hashs
	command2 := rabbitmq.WriteHashCommand{
		BucketName: bucketName,
		ObjectName: objectName,
		Hashs:      hashs,
		Ips:        ips,
		UserId:     userId,
	}
	c1, _ = json.Marshal(command2)
	b1 = append([]byte("01"), c1...)
	rabbit1.PublishSimple(b1)

	//接受第二轮通讯结果
	var res2 rabbitmq.WriteHashRes
	msgs = rabbit2.ConsumeSimple(time.Millisecond, true)
	wg.Add(1)
	go func() {
		for d := range msgs {
			_ = json.Unmarshal(d.Body, &res2)
			if res2.MetaCode == 0 {
				wg.Done()
			}
		}
	}()
	wg.Wait()
	rabbit1.Destroy()
	rabbit2.Destroy()
	//
}

func repMove(ip string, hash string) {
	//TO DO: 通过消息队列发送调度命令
}

func ecMove(ip string, hashs []string, ids []int, ecName string) {
	//TO DO: 通过消息队列发送调度命令
}

func loadDistribute(localFilePath string, loadDistributeBufs []chan []byte, numWholePacket int64, lastPacketInBytes int64) {
	fmt.Println("loadDistribute " + localFilePath)
	file, _ := os.Open(localFilePath)
	for i := 0; int64(i) < numWholePacket; i++ {
		buf := make([]byte, packetSizeInBytes)
		_, err := file.Read(buf)
		if err != nil && err != io.EOF {
			break
		}
		for j := 0; j < len(loadDistributeBufs); j++ {
			loadDistributeBufs[j] <- buf
		}
	}
	if lastPacketInBytes > 0 {
		buf := make([]byte, lastPacketInBytes)
		file.Read(buf)
		for j := 0; j < len(loadDistributeBufs); j++ {
			loadDistributeBufs[j] <- buf
		}
	}
	fmt.Println("load over")
	for i := 0; i < len(loadDistributeBufs); i++ {
		close(loadDistributeBufs[i])
	}
	file.Close()
}

func load(localFilePath string, loadBufs []chan []byte, ecK int, totalNumPacket int64, fileSizeInBytes int64) {
	fmt.Println("load " + localFilePath)
	file, _ := os.Open(localFilePath)

	for i := 0; int64(i) < totalNumPacket; i++ {
		print(totalNumPacket)

		buf := make([]byte, packetSizeInBytes)
		idx := i % ecK
		print(len(loadBufs))
		_, err := file.Read(buf)
		loadBufs[idx] <- buf

		if idx == ecK-1 {
			print("***")
			for j := ecK; j < len(loadBufs); j++ {
				print(j)
				zeroPkt := make([]byte, packetSizeInBytes)
				fmt.Printf("%v", zeroPkt)
				loadBufs[j] <- zeroPkt
			}
		}
		if err != nil && err != io.EOF {
			break
		}
	}
	fmt.Println("load over")
	for i := 0; i < len(loadBufs); i++ {
		print(i)
		close(loadBufs[i])
	}
	file.Close()
}

func encode(inBufs []chan []byte, outBufs []chan []byte, ecK int, coefs [][]int64, numPacket int64) {
	fmt.Println("encode ")
	var tmpIn [][]byte
	tmpIn = make([][]byte, len(outBufs))
	enc := ec.NewRsEnc(ecK, len(outBufs))
	for i := 0; int64(i) < numPacket; i++ {
		for j := 0; j < len(outBufs); j++ { //3
			tmpIn[j] = <-inBufs[j]
			//print(i)
			//fmt.Printf("%v",tmpIn[j])
			//print("@#$")
		}
		enc.Encode(tmpIn)
		fmt.Printf("%v", tmpIn)
		print("$$$$$$$$$$$$$$$$$$")
		for j := 0; j < len(outBufs); j++ { //1,2,3//示意，需要调用纠删码编解码引擎：  tmp[k] = tmp[k]+(tmpIn[w][k]*coefs[w][j])
			outBufs[j] <- tmpIn[j]
		}
	}
	fmt.Println("encode over")
	for i := 0; i < len(outBufs); i++ {
		close(outBufs[i])
	}
}

func decode(inBufs []chan []byte, outBufs []chan []byte, blockSeq []int, ecK int, coefs [][]int64, numPacket int64) {
	fmt.Println("decode ")
	var tmpIn [][]byte
	var zeroPkt []byte
	tmpIn = make([][]byte, len(inBufs))
	hasBlock := map[int]bool{}
	for j := 0; j < len(blockSeq); j++ {
		hasBlock[blockSeq[j]] = true
	}
	needRepair := false //检测是否传入了所有数据块
	for j := 0; j < len(outBufs); j++ {
		if blockSeq[j] != j {
			needRepair = true
		}
	}
	enc := ec.NewRsEnc(ecK, len(inBufs))
	for i := 0; int64(i) < numPacket; i++ {
		for j := 0; j < len(inBufs); j++ { //3
			if hasBlock[j] {
				tmpIn[j] = <-inBufs[j]
			} else {
				tmpIn[j] = zeroPkt
			}
		}
		fmt.Printf("%v", tmpIn)
		if needRepair {
			err := enc.Repair(tmpIn)
			print("&&&&&")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Decode Repair Error: %s", err.Error())
			}
		}
		//fmt.Printf("%v",tmpIn)

		for j := 0; j < len(outBufs); j++ { //1,2,3//示意，需要调用纠删码编解码引擎：  tmp[k] = tmp[k]+(tmpIn[w][k]*coefs[w][j])
			outBufs[j] <- tmpIn[j]
		}
	}
	fmt.Println("decode over")
	for i := 0; i < len(outBufs); i++ {
		close(outBufs[i])
	}
}

func send(blockName string, ip string, inBuf chan []byte, numPacket int64, wg *sync.WaitGroup, hashs []string, idx int) {
	fmt.Println("send " + blockName)
	/*
		TO DO ss: 判断本地有没有ipfs daemon、能否与目标agent的ipfs daemon连通、本地ipfs目录空间是否充足
			如果本地有ipfs daemon、能与目标agent的ipfs daemon连通、本地ipfs目录空间充足，将所有内容写入本地ipfs目录，得到对象的cid，发送cid给目标agent让其pin相应的对象
			否则，像目前一样，使用grpc向指定节点获取
	*/
	//rpc相关
	conn, err := grpc.Dial(ip+port, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	client := agentcaller.NewTranBlockOrReplicaClient(conn)
	stream, err := client.SendBlockOrReplica(context.Background())
	if err != nil {
		panic(err)
	}
	for i := 0; int64(i) < numPacket; i++ {
		buf := <-inBuf
		fmt.Println(buf)
		err := stream.Send(&agentcaller.BlockOrReplica{
			BlockOrReplicaName: blockName,
			BlockOrReplicaHash: blockName,
			BlockOrReplicaData: buf,
		})
		if err != nil && err != io.EOF {
			panic(err)
		}
	}
	res, err := stream.CloseAndRecv()
	fmt.Println(res)
	hashs[idx] = res.BlockOrReplicaHash
	conn.Close()
	wg.Done()
	return
}

func get(blockHash string, ip string, getBuf chan []byte, numPacket int64) {
	//rpc相关
	print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
	conn, err := grpc.Dial(ip+port, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	/*
		TO DO: 判断本地有没有ipfs daemon、能否获取相应对象的cid
			如果本地有ipfs daemon且能获取相应编码块的cid，则获取编码块cid对应的ipfsblock的cid，通过ipfs网络获取这些ipfsblock
			否则，像目前一样，使用grpc向指定节点获取
	*/
	client := agentcaller.NewTranBlockOrReplicaClient(conn)
	//rpc get
	stream, _ := client.GetBlockOrReplica(context.Background(), &agentcaller.GetReq{
		BlockOrReplicaHash: blockHash,
	})
	fmt.Println(numPacket)
	for i := 0; int64(i) < numPacket; i++ {
		fmt.Println(i)
		res, _ := stream.Recv()
		fmt.Println(res.BlockOrReplicaData)
		getBuf <- res.BlockOrReplicaData
	}
	close(getBuf)
	conn.Close()
}

func persist(inBuf []chan []byte, numPacket int64, localFilePath string, wg *sync.WaitGroup) {
	fDir, err := os.Executable()
	if err != nil {
		panic(err)
	}
	fURL := filepath.Join(filepath.Dir(fDir), "assets")
	_, err = os.Stat(fURL)
	if os.IsNotExist(err) {
		os.MkdirAll(fURL, os.ModePerm)
	}

	file, err := os.Create(filepath.Join(fURL, localFilePath))
	if err != nil {
		return
	}

	for i := 0; int64(i) < numPacket; i++ {
		for j := 0; j < len(inBuf); j++ {
			tmp := <-inBuf[j]
			fmt.Println(tmp)
			file.Write(tmp)
		}
	}
	file.Close()
	wg.Done()
}
