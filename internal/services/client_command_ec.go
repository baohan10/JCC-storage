package services

// TODO 将这里的逻辑拆分到services中实现

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/ec"
	"gitlink.org.cn/cloudream/utils"
)

func EcWrite(localFilePath string, bucketID int, objectName string, ecName string) error {
	panic("not implement yet!")
	/*
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
		writeECHashResp, err := coorClient.WriteECHash(bucketName, objectName, hashs, ecWriteResp.NodeIPs, userId)
		if err != nil {
			return fmt.Errorf("request to coordinator failed, err: %w", err)
		}
		if writeECHashResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("coordinator WriteECHash failed, err: %w", err)
		}

		return nil
	*/
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
	panic("not implement yet!")

	/*
		//	TO DO ss: 判断本地有没有ipfs daemon、能否与目标agent的ipfs daemon连通、本地ipfs目录空间是否充足
		//	如果本地有ipfs daemon、能与目标agent的ipfs daemon连通、本地ipfs目录空间充足，将所有内容写入本地ipfs目录，得到对象的cid，发送cid给目标agent让其pin相应的对象
		//	否则，像目前一样，使用grpc向指定节点获取

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
	*/
}

func get(blockHash string, nodeIP string, getBuf chan []byte, numPacket int64) error {
	panic("not implement yet!")
	/*
		grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
		conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
		}
		defer conn.Close()
		// TO DO: 判断本地有没有ipfs daemon、能否获取相应对象的cid
		// 如果本地有ipfs daemon且能获取相应编码块的cid，则获取编码块cid对应的ipfsblock的cid，通过ipfs网络获取这些ipfsblock
		// 否则，像目前一样，使用grpc向指定节点获取
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
	*/
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
