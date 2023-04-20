package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	"google.golang.org/grpc/credentials/insecure"
)

func Move(bucketName string, objectName string, stgID int) error {
	// TODO 此处是写死的常量
	userId := 0

	// 先向协调端请求文件相关的元数据
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	moveResp, err := coorClient.Move(bucketName, objectName, userId, stgID)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if moveResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator operation failed, code: %s, message: %s", moveResp.ErrorCode, moveResp.Message)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := racli.NewAgentClient(moveResp.NodeID)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", stgID, err)
	}
	defer agentClient.Close()

	switch moveResp.Redundancy {
	case consts.REDUNDANCY_REP:
		agentMoveResp, err := agentClient.RepMove(moveResp.Directory, moveResp.Hashes, bucketName, objectName, userId, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", stgID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", stgID, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}

	case consts.REDUNDANCY_EC:
		agentMoveResp, err := agentClient.ECMove(moveResp.Directory, moveResp.Hashes, moveResp.IDs, moveResp.ECName, bucketName, objectName, userId, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", stgID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", stgID, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}
	}

	return nil
}

func Read(localFilePath string, bucketName string, objectName string) error {
	// TODO 此处是写死的常量
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
	case consts.REDUNDANCY_REP:
		if len(readResp.NodeIPs) == 0 {
			return fmt.Errorf("no node has this file")
		}

		// 随便选第一个节点下载文件
		err = repRead(readResp.FileSizeInBytes, readResp.NodeIPs[0], readResp.Hashes[0], localFilePath)
		if err != nil {
			return fmt.Errorf("rep read failed, err: %w", err)
		}

	case consts.REDUNDANCY_EC:
		// TODO EC部分的代码要考虑重构
		ecRead(readResp.FileSizeInBytes, readResp.NodeIPs, readResp.Hashes, readResp.BlockIDs, readResp.ECName, localFilePath)
	}

	return nil
}

func repRead(fileSizeInBytes int64, nodeIP string, repHash string, localFilePath string) error {
	// 连接grpc
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}
	defer conn.Close()

	// 创建本地文件
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
	// 下载文件
	client := agentcaller.NewFileTransportClient(conn)
	stream, err := client.GetFile(context.Background(), &agentcaller.GetReq{
		FileHash: repHash,
	})
	if err != nil {
		return fmt.Errorf("request grpc failed, err: %w", err)
	}
	defer stream.CloseSend()

	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("read file data on grpc stream failed, err: %w", err)
		}

		if resp.Type == agentcaller.FileDataPacketType_Data {
			err = myio.WriteAll(outputFile, resp.Data)
			// TODO 写入到文件失败，是否要考虑删除这个不完整的文件？
			if err != nil {
				return fmt.Errorf("write file data to local file failed, err: %w", err)
			}

		} else if resp.Type == agentcaller.FileDataPacketType_EOF {
			return nil
		}
	}
}

func ecRead(fileSizeInBytes int64, nodeIPs []string, blockHashs []string, blockIds []int, ecName string, localFilePath string) {
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
		go get(blockHashs[i], nodeIPs[i], getBufs[blockSeq[i]], numPacket)
	}
	go decode(getBufs[:], decodeBufs[:], blockSeq, ecK, coefs, numPacket)
	go persist(decodeBufs[:], numPacket, localFilePath, &wg)
	wg.Wait()
}

type fileSender struct {
	grpcCon  *grpc.ClientConn
	stream   agentcaller.FileTransport_SendFileClient
	nodeID   int
	fileHash string
	err      error
}

func RepWrite(localFilePath string, bucketName string, objectName string, numRep int) error {
	// TODO 此处是写死的常量
	userId := 0

	//获取文件大小
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return fmt.Errorf("get file %s state failed, err: %w", localFilePath, err)
	}
	fileSizeInBytes := fileInfo.Size()

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

	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("open file %s failed, err: %w", localFilePath, err)
	}
	defer file.Close()

	/*
		TO DO ss: 判断本地有没有ipfs daemon、能否与目标agent的ipfs daemon连通、本地ipfs目录空间是否充足
			如果本地有ipfs daemon、能与目标agent的ipfs daemon连通、本地ipfs目录空间充足，将所有内容写入本地ipfs目录，得到对象的cid，发送cid给目标agent让其pin相应的对象
			否则，像目前一样，使用grpc向指定节点获取
	*/

	senders := make([]fileSender, numRep)

	// 建立grpc连接，发送请求
	startSendFile(numRep, senders, repWriteResp.NodeIDs, repWriteResp.NodeIPs)

	// 向每个节点发送数据
	err = sendFileData(file, numRep, senders)
	if err != nil {
		return err
	}

	// 发送EOF消息，并获得FileHash
	sendFinish(numRep, senders)

	// 收集发送成功的节点以及返回的hash
	var sucNodeIDs []int
	var sucFileHashes []string
	for i := 0; i < numRep; i++ {
		sender := &senders[i]

		if sender.err == nil {
			sucNodeIDs = append(sucNodeIDs, sender.nodeID)
			sucFileHashes = append(sucFileHashes, sender.fileHash)
		}
	}

	// 记录写入的文件的Hash
	// TODO 如果一个都没有写成功，那么是否要发送这个请求？
	writeRepHashResp, err := coorClient.WriteRepHash(bucketName, objectName, fileSizeInBytes, numRep, userId, sucNodeIDs, sucFileHashes)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if writeRepHashResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator WriteRepHash failed, err: %w", err)
	}

	return nil
}

func startSendFile(numRep int, senders []fileSender, nodeIDs []int, nodeIPs []string) {
	for i := 0; i < numRep; i++ {
		sender := &senders[i]

		sender.nodeID = nodeIDs[i]

		grpcAddr := fmt.Sprintf("%s:%d", nodeIPs[i], config.Cfg().GRPCPort)
		conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			sender.err = fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
			continue
		}

		client := agentcaller.NewFileTransportClient(conn)
		stream, err := client.SendFile(context.Background())
		if err != nil {
			conn.Close()
			sender.err = fmt.Errorf("request to send file failed, err: %w", err)
			continue
		}

		sender.grpcCon = conn
		sender.stream = stream
	}
}

func sendFileData(file *os.File, numRep int, senders []fileSender) error {

	// 共用的发送数据缓冲区
	buf := make([]byte, 2048)

	for {
		// 读取文件数据
		readCnt, err := file.Read(buf)

		// 文件读取完毕
		if err == io.EOF {
			break
		}

		if err != nil {
			// 读取失败则断开所有连接
			for i := 0; i < numRep; i++ {
				sender := &senders[i]

				if sender.err != nil {
					continue
				}

				sender.stream.CloseSend()
				sender.grpcCon.Close()
				sender.err = fmt.Errorf("read file data failed, err: %w", err)
			}

			return fmt.Errorf("read file data failed, err: %w", err)
		}

		// 并行的向每个节点发送数据
		hasSender := false
		var sendWg sync.WaitGroup
		for i := 0; i < numRep; i++ {
			sender := &senders[i]

			// 发生了错误的跳过
			if sender.err != nil {
				continue
			}

			hasSender = true

			sendWg.Add(1)
			go func() {
				err := sender.stream.Send(&agentcaller.FileDataPacket{
					Type: agentcaller.FileDataPacketType_Data,
					Data: buf[:readCnt],
				})

				// 发生错误则关闭连接
				if err != nil {
					sender.stream.CloseSend()
					sender.grpcCon.Close()
					sender.err = fmt.Errorf("send file data failed, err: %w", err)
				}

				sendWg.Done()
			}()
		}

		// 等待向每个节点发送数据结束
		sendWg.Wait()

		// 如果所有节点都发送失败，则不要再继续读取文件数据了
		if !hasSender {
			break
		}
	}
	return nil
}

func sendFinish(numRep int, senders []fileSender) {
	for i := 0; i < numRep; i++ {
		sender := &senders[i]

		// 发生了错误的跳过
		if sender.err != nil {
			continue
		}

		err := sender.stream.Send(&agentcaller.FileDataPacket{
			Type: agentcaller.FileDataPacketType_EOF,
		})
		if err != nil {
			sender.stream.CloseSend()
			sender.grpcCon.Close()
			sender.err = fmt.Errorf("send file data failed, err: %w", err)
			continue
		}

		resp, err := sender.stream.CloseAndRecv()
		if err != nil {
			sender.err = fmt.Errorf("receive response failed, err: %w", err)
			sender.grpcCon.Close()
			continue
		}

		sender.fileHash = resp.FileHash
		sender.grpcCon.Close()
	}
}

func EcWrite(localFilePath string, bucketName string, objectName string, ecName string) error {
	fmt.Println("write " + localFilePath + " as " + bucketName + "/" + objectName)

	// TODO 需要参考RepWrite函数的代码逻辑，做好错误处理

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
	hashs := make([]string, ecN)
	//正式开始写入
	go load(localFilePath, loadBufs[:ecN], ecK, numPacket*int64(ecK), fileSizeInBytes) //从本地文件系统加载数据
	go encode(loadBufs[:ecN], encodeBufs[:ecN], ecK, coefs, numPacket)

	var wg sync.WaitGroup
	wg.Add(ecN)

	for i := 0; i < ecN; i++ {
		go send(ecWriteResp.NodeIPs[i], encodeBufs[i], numPacket, &wg, hashs, i)
	}
	wg.Wait()

	//第二轮通讯:插入元数据hashs
	writeRepHashResp, err := coorClient.WriteECHash(bucketName, objectName, hashs, ecWriteResp.NodeIPs, userId)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if writeRepHashResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator WriteECHash failed, err: %w", err)
	}

	return nil
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

func send(ip string, inBuf chan []byte, numPacket int64, wg *sync.WaitGroup, hashs []string, idx int) error {
	/*
		TO DO ss: 判断本地有没有ipfs daemon、能否与目标agent的ipfs daemon连通、本地ipfs目录空间是否充足
			如果本地有ipfs daemon、能与目标agent的ipfs daemon连通、本地ipfs目录空间充足，将所有内容写入本地ipfs目录，得到对象的cid，发送cid给目标agent让其pin相应的对象
			否则，像目前一样，使用grpc向指定节点获取
	*/

	// TODO 如果发生错误，需要考虑将错误传递出去
	defer wg.Done()

	grpcAddr := fmt.Sprintf("%s:%d", ip, config.Cfg().GRPCPort)
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}
	defer conn.Close()

	client := agentcaller.NewFileTransportClient(conn)
	stream, err := client.SendFile(context.Background())
	if err != nil {
		return fmt.Errorf("request to send file failed, err: %w", err)
	}

	for i := 0; int64(i) < numPacket; i++ {
		buf := <-inBuf

		err := stream.Send(&agentcaller.FileDataPacket{
			Code: agentcaller.FileDataPacket_OK,
			Data: buf,
		})

		if err != nil {
			stream.CloseSend()
			return fmt.Errorf("send file data failed, err: %w", err)
		}
	}

	err = stream.Send(&agentcaller.FileDataPacket{
		Code: agentcaller.FileDataPacket_EOF,
	})

	if err != nil {
		stream.CloseSend()
		return fmt.Errorf("send file data failed, err: %w", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("receive response failed, err: %w", err)
	}

	hashs[idx] = resp.FileHash
	return nil
}

func get(blockHash string, nodeIP string, getBuf chan []byte, numPacket int64) error {
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}
	defer conn.Close()
	/*
		TO DO: 判断本地有没有ipfs daemon、能否获取相应对象的cid
			如果本地有ipfs daemon且能获取相应编码块的cid，则获取编码块cid对应的ipfsblock的cid，通过ipfs网络获取这些ipfsblock
			否则，像目前一样，使用grpc向指定节点获取
	*/
	client := agentcaller.NewFileTransportClient(conn)
	//rpc get
	// TODO 要考虑读取失败后，如何中断后续解码过程
	stream, err := client.GetFile(context.Background(), &agentcaller.GetReq{
		FileHash: blockHash,
	})

	for i := 0; int64(i) < numPacket; i++ {
		fmt.Println(i)
		// TODO 同上
		res, _ := stream.Recv()
		fmt.Println(res.BlockOrReplicaData)
		getBuf <- res.BlockOrReplicaData
	}

	close(getBuf)
	return nil
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
