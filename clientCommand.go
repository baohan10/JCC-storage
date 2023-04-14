package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"gitlink.org.cn/cloudream/client/config"
	agentcaller "gitlink.org.cn/cloudream/proto"

	racli "gitlink.org.cn/cloudream/rabbitmq/client"
	"gitlink.org.cn/cloudream/utils"
	"gitlink.org.cn/cloudream/utils/consts"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
	myio "gitlink.org.cn/cloudream/utils/io"

	"gitlink.org.cn/cloudream/ec"

	"google.golang.org/grpc"

	_ "google.golang.org/grpc/balancer/grpclb"
)

func Move(bucketName string, objectName string, destination string) error {
	//将bucketName, objectName, destination发给协调端
	fmt.Println("move " + bucketName + "/" + objectName + " to " + destination)
	userId := 0

	//获取块hash，ip，序号，编码参数等
	//发送写请求，分配写入节点Ip
	// 先向协调端请求文件相关的数据
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	moveResp, err := coorClient.Move(bucketName, objectName, userId, destination)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if moveResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator operation failed, code: %s, message: %s", moveResp.ErrorCode, moveResp.Message)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := racli.NewAgentClient(destination)
	if err != nil {
		return fmt.Errorf("create agent client to %s failed, err: %w", destination, err)
	}
	defer agentClient.Close()

	switch moveResp.Redundancy {
	case consts.REDUNDANCY_REP:
		agentMoveResp, err := agentClient.RepMove(moveResp.Hashes, bucketName, objectName, userId, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %s failed, err: %w", destination, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %s operation failed, code: %s, messsage: %s", destination, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}

	case consts.REDUNDANCY_EC:
		agentMoveResp, err := agentClient.ECMove(moveResp.Hashes, moveResp.IDs, moveResp.ECName, bucketName, objectName, userId, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %s failed, err: %w", destination, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %s operation failed, code: %s, messsage: %s", destination, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}
	}

	return nil
}

func Read(localFilePath string, bucketName string, objectName string) error {
	fmt.Println("read " + bucketName + "/" + objectName + " to " + localFilePath)
	//获取块hash，ip，序号，编码参数等
	//发送写请求，分配写入节点Ip
	userId := 0

	// 先向协调端请求文件相关的数据
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	readResp, err := coorClient.Read(bucketName, objectName, userId)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if readResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator operation failed, code: %s, message: %s", readResp.ErrorCode, readResp.Message)
	}

	switch readResp.Redundancy {
	case "rep":
		err = repRead(readResp.FileSizeInBytes, readResp.IPs[0], readResp.Hashes[0], localFilePath)
		if err != nil {
			return fmt.Errorf("rep read failed, err: %w", err)
		}

	case "ec":
		// TODO EC部分的代码要考虑重构
		ecRead(readResp.FileSizeInBytes, readResp.IPs, readResp.Hashes, readResp.BlockIDs, readResp.ECName, localFilePath)
	}

	return nil
}

func repRead(fileSizeInBytes int64, ip string, repHash string, localFilePath string) error {
	grpcAddr := fmt.Sprintf("%s:%d", ip, config.Cfg().GRPCPort)
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}
	defer conn.Close()

	client := agentcaller.NewTranBlockOrReplicaClient(conn)

	curExecPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable directory failed, err: %w", err)
	}

	outputFilePath := filepath.Join(filepath.Dir(curExecPath), localFilePath)
	outputFileDir := filepath.Dir(outputFilePath)

	err = os.MkdirAll(outputFileDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create output file directory %s failed, err: %w", outputFileDir, err)
	}

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("create output file %s failed, err: %w", outputFilePath, err)
	}
	defer outputFile.Close()

	/*
		TO DO: 判断本地有没有ipfs daemon、能否获取相应对象的cid
			如果本地有ipfs daemon且能获取相应对象的cid，则获取对象cid对应的ipfsblock的cid，通过ipfs网络获取这些ipfsblock
			否则，像目前一样，使用grpc向指定节点获取
	*/
	stream, err := client.GetBlockOrReplica(context.Background(), &agentcaller.GetReq{
		BlockOrReplicaHash: repHash,
	})
	if err != nil {
		return fmt.Errorf("request grpc failed, err: %w", err)
	}

	numPacket := (fileSizeInBytes + config.Cfg().GRCPPacketSize - 1) / config.Cfg().GRCPPacketSize
	for i := int64(0); i < numPacket; i++ {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("read file data on grpc stream failed, err: %w", err)
		}

		err = myio.WriteAll(outputFile, resp.BlockOrReplicaData)
		if err != nil {
			return fmt.Errorf("write file data to local file failed, err: %w", err)
		}
	}

	return nil
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

	numPacket := (fileSizeInBytes + int64(ecK)*config.Cfg().GRCPPacketSize - 1) / (int64(ecK) * config.Cfg().GRCPPacketSize)
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

func RepWrite(localFilePath string, bucketName string, objectName string, numRep int) error {
	userId := 0
	//获取文件大小
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return fmt.Errorf("get file %s state failed, err: %w", localFilePath, err)
	}
	fileSizeInBytes := fileInfo.Size()

	//写入对象的packet数
	numWholePacket := fileSizeInBytes / config.Cfg().GRCPPacketSize
	lastPacketInBytes := fileSizeInBytes % config.Cfg().GRCPPacketSize
	numPacket := numWholePacket
	if lastPacketInBytes > 0 {
		numPacket++
	}

	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	//发送写请求，请求Coor分配写入节点Ip
	repWriteResp, err := coorClient.RepWrite(bucketName, objectName, fileSizeInBytes, numRep, userId)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if repWriteResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator RepWrite failed, err: %w", err)
	}

	//创建channel
	loadDistributeBufs := make([]chan []byte, numRep)
	for i := 0; i < numRep; i++ {
		loadDistributeBufs[i] = make(chan []byte)
	}

	//正式开始写入
	hashs := make([]string, numRep)
	go loadDistribute(localFilePath, loadDistributeBufs[:], numWholePacket, lastPacketInBytes) //从本地文件系统加载数据
	var wg sync.WaitGroup
	wg.Add(numRep)
	for i := 0; i < numRep; i++ {
		//TODO xh: send的第一个参数不需要了
		// TODO2 见上
		go send("rep.json"+strconv.Itoa(i), repWriteResp.IPs[i], loadDistributeBufs[i], numPacket, &wg, hashs, i) //"block1.json"这样参数不需要
	}
	wg.Wait()

	// 记录写入的文件的Hash
	writeRepHashResp, err := coorClient.WriteRepHash(bucketName, objectName, hashs, repWriteResp.IPs, userId)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if writeRepHashResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator WriteRepHash failed, err: %w", err)
	}

	return nil
}

func EcWrite(localFilePath string, bucketName string, objectName string, ecName string) error {
	fmt.Println("write " + localFilePath + " as " + bucketName + "/" + objectName)

	//获取文件大小
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return fmt.Errorf("get file %s state failed, err: %w", localFilePath, err)
	}
	fileSizeInBytes := fileInfo.Size()

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
	numPacket := (fileSizeInBytes + int64(ecK)*config.Cfg().GRCPPacketSize - 1) / (int64(ecK) * config.Cfg().GRCPPacketSize)
	fmt.Println(numPacket)

	userId := 0
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	//发送写请求，请求Coor分配写入节点Ip
	ecWriteResp, err := coorClient.ECWrite(bucketName, objectName, fileSizeInBytes, ecName, userId)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if ecWriteResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator ECWrite failed, err: %w", err)
	}

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

	var wg sync.WaitGroup
	wg.Add(ecN)

	for i := 0; i < ecN; i++ {
		go send(blockNames[i], ecWriteResp.IPs[i], encodeBufs[i], numPacket, &wg, hashs, i)
	}
	wg.Wait()

	//第二轮通讯:插入元数据hashs
	writeRepHashResp, err := coorClient.WriteECHash(bucketName, objectName, hashs, ecWriteResp.IPs, userId)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if writeRepHashResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator WriteECHash failed, err: %w", err)
	}

	return nil
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
		buf := make([]byte, config.Cfg().GRCPPacketSize)
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

		buf := make([]byte, config.Cfg().GRCPPacketSize)
		idx := i % ecK
		print(len(loadBufs))
		_, err := file.Read(buf)
		loadBufs[idx] <- buf

		if idx == ecK-1 {
			print("***")
			for j := ecK; j < len(loadBufs); j++ {
				print(j)
				zeroPkt := make([]byte, config.Cfg().GRCPPacketSize)
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
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip, config.Cfg().GRPCPort), grpc.WithInsecure())
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
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip, config.Cfg().GRPCPort), grpc.WithInsecure())
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
