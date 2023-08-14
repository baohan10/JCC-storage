package services

// TODO 将这里的逻辑拆分到services中实现

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gitlink.org.cn/cloudream/common/utils"
	"gitlink.org.cn/cloudream/ec"
	"gitlink.org.cn/cloudream/storage-client/internal/config"

	//"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	mygrpc "gitlink.org.cn/cloudream/common/utils/grpc"
	agentcaller "gitlink.org.cn/cloudream/proto"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (svc *ObjectService) UploadEcObject(userID int64, bucketID int64, objectName string, file io.ReadCloser, fileSize int64, ecName string) error {
	// TODO 需要加锁

	/*reqBlder := reqbuilder.NewBuilder()
	for _, uploadObject := range t.Objects {
		reqBlder.Metadata().
			// 用于防止创建了多个同名对象
			Object().CreateOne(t.bucketID, uploadObject.ObjectName)
	}*/
	/*
		mutex, err := reqBlder.
			Metadata().
			// 用于判断用户是否有桶的权限
			UserBucket().ReadOne(userID, bucketID).
			// 用于查询可用的上传节点
			Node().ReadAny().
			// 用于设置Rep配置
			ObjectRep().CreateAny().
			// 用于创建Cache记录
			Cache().CreateAny().
			MutexLock(ctx.DistLock)
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()
	*/
	//发送写请求，请求Coor分配写入节点Ip
	ecWriteResp, err := svc.coordinator.PreUploadEcObject(coormsg.NewPreUploadEcObject(bucketID, objectName, fileSize, ecName, userID, config.Cfg().ExternalIP))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}

	if len(ecWriteResp.Nodes) == 0 {
		return fmt.Errorf("no node to upload file")
	}
	//生成纠删码的写入节点序列
	nodes := make([]ramsg.RespNode, ecWriteResp.Ec.EcN)
	numNodes := len(ecWriteResp.Nodes)
	startWriteNodeID := rand.Intn(numNodes)
	for i := 0; i < ecWriteResp.Ec.EcN; i++ {
		nodes[i] = ecWriteResp.Nodes[(startWriteNodeID+i)%numNodes]
	}
	hashs, err := svc.ecWrite(file, fileSize, ecWriteResp.Ec.EcK, ecWriteResp.Ec.EcN, nodes)
	if err != nil {
		return fmt.Errorf("EcWrite failed, err: %w", err)
	}
	nodeIDs := make([]int64, len(nodes))
	for i := 0; i < len(nodes); i++ {
		nodeIDs[i] = nodes[i].ID
	}
	//第二轮通讯:插入元数据hashs
	dirName := utils.GetDirectoryName(objectName)
	_, err = svc.coordinator.CreateEcObject(coormsg.NewCreateEcObject(bucketID, objectName, fileSize, userID, nodeIDs, hashs, ecName, dirName))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	return nil
}

func (svc *ObjectService) ecWrite(file io.ReadCloser, fileSize int64, ecK int, ecN int, nodes []ramsg.RespNode) ([]string, error) {

	// TODO 需要参考RepWrite函数的代码逻辑，做好错误处理
	//获取文件大小

	var coefs = [][]int64{{1, 1, 1}, {1, 2, 3}} //2应替换为ecK，3应替换为ecN
	//计算每个块的packet数
	numPacket := (fileSize + int64(ecK)*config.Cfg().ECPacketSize - 1) / (int64(ecK) * config.Cfg().ECPacketSize)
	//fmt.Println(numPacket)
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
	go load(file, loadBufs[:ecN], ecK, numPacket*int64(ecK)) //从本地文件系统加载数据
	go encode(loadBufs[:ecN], encodeBufs[:ecN], ecK, coefs, numPacket)

	var wg sync.WaitGroup
	wg.Add(ecN)
	/*mutex, err := reqbuilder.NewBuilder().
		// 防止上传的副本被清除
		IPFS().CreateAnyRep(node.ID).
		MutexLock(svc.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()
	*/
	for i := 0; i < ecN; i++ {
		go svc.send(nodes[i], encodeBufs[i], numPacket, &wg, hashs, i)
	}
	wg.Wait()

	return hashs, nil

}

func (svc *ObjectService) downloadEcObject(fileSize int64, ecK int, ecN int, blockIDs []int, nodeIDs []int64, nodeIPs []string, hashs []string) (io.ReadCloser, error) {
	// TODO zkx 先试用同步方式实现逻辑，做好错误处理。同时也方便下面直接使用uploadToNode和uploadToLocalIPFS来优化代码结构
	//wg := sync.WaitGroup{}
	numPacket := (fileSize + int64(ecK)*config.Cfg().ECPacketSize - 1) / (int64(ecK) * config.Cfg().ECPacketSize)
	getBufs := make([]chan []byte, ecN)
	decodeBufs := make([]chan []byte, ecK)
	for i := 0; i < ecN; i++ {
		getBufs[i] = make(chan []byte)
	}
	for i := 0; i < ecK; i++ {
		decodeBufs[i] = make(chan []byte)
	}
	for i := 0; i < len(blockIDs); i++ {
		go svc.get(hashs[i], nodeIPs[i], getBufs[blockIDs[i]], numPacket)
	}
	print(numPacket)
	go decode(getBufs[:], decodeBufs[:], blockIDs, ecK, numPacket)
	r, w := io.Pipe()
	//persist函数,将解码得到的文件写入pipe
	go func() {
		for i := 0; int64(i) < numPacket; i++ {
			for j := 0; j < len(decodeBufs); j++ {
				tmp := <-decodeBufs[j]
				_, err := w.Write(tmp)
				if err != nil {
					fmt.Errorf("persist file falied, err:%w", err)
				}
			}
		}
		w.Close()
	}()
	return r, nil
}

func (svc *ObjectService) get(blockHash string, nodeIP string, getBuf chan []byte, numPacket int64) error {
	downloadFromAgent := false
	//使用本地IPFS获取
	if svc.ipfs != nil {
		log.Infof("try to use local IPFS to download file")
		//获取IPFS的reader
		reader, err := svc.downloadFromLocalIPFS(blockHash)
		if err != nil {
			downloadFromAgent = true
			fmt.Errorf("read ipfs block failed, err: %w", err)
		}
		defer reader.Close()
		for i := 0; int64(i) < numPacket; i++ {
			buf := make([]byte, config.Cfg().ECPacketSize)
			_, err := io.ReadFull(reader, buf)
			if err != nil {
				downloadFromAgent = true
				fmt.Errorf("read file falied, err:%w", err)
			}
			getBuf <- buf
		}
		if downloadFromAgent == false {
			close(getBuf)
			return nil
		}
	} else {
		downloadFromAgent = true
	}
	//从agent获取
	if downloadFromAgent == true {
		/*// 二次获取锁
		mutex, err := reqbuilder.NewBuilder().
			// 用于从IPFS下载文件
			IPFS().ReadOneRep(nodeID, fileHash).
			MutexLock(svc.distlock)
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()
		*/
		// 连接grpc
		grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
		conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
		}
		// 下载文件
		client := agentcaller.NewFileTransportClient(conn)
		reader, err := mygrpc.GetFileAsStream(client, blockHash)
		if err != nil {
			conn.Close()
			return fmt.Errorf("request to get file failed, err: %w", err)
		}
		for i := 0; int64(i) < numPacket; i++ {
			buf := make([]byte, config.Cfg().ECPacketSize)
			_, _ = reader.Read(buf)
			fmt.Println(buf)
			fmt.Println(numPacket, "\n")
			getBuf <- buf
		}
		close(getBuf)
		reader.Close()
		return nil
	}
	return nil

}

func load(file io.ReadCloser, loadBufs []chan []byte, ecK int, totalNumPacket int64) error {

	for i := 0; int64(i) < totalNumPacket; i++ {

		buf := make([]byte, config.Cfg().ECPacketSize)
		idx := i % ecK
		_, err := file.Read(buf)
		if err != nil {
			return fmt.Errorf("read file falied, err:%w", err)
		}
		loadBufs[idx] <- buf

		if idx == ecK-1 {
			for j := ecK; j < len(loadBufs); j++ {
				zeroPkt := make([]byte, config.Cfg().ECPacketSize)
				loadBufs[j] <- zeroPkt
			}
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("load file to buf failed, err:%w", err)
		}
	}
	for i := 0; i < len(loadBufs); i++ {

		close(loadBufs[i])
	}
	file.Close()
	return nil
}

func encode(inBufs []chan []byte, outBufs []chan []byte, ecK int, coefs [][]int64, numPacket int64) {
	var tmpIn [][]byte
	tmpIn = make([][]byte, len(outBufs))
	enc := ec.NewRsEnc(ecK, len(outBufs))
	for i := 0; int64(i) < numPacket; i++ {
		for j := 0; j < len(outBufs); j++ {
			tmpIn[j] = <-inBufs[j]
		}
		enc.Encode(tmpIn)
		for j := 0; j < len(outBufs); j++ {
			outBufs[j] <- tmpIn[j]
		}
	}
	for i := 0; i < len(outBufs); i++ {
		close(outBufs[i])
	}
}

func decode(inBufs []chan []byte, outBufs []chan []byte, blockSeq []int, ecK int, numPacket int64) {
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
		print("!!!!!")
		for j := 0; j < len(inBufs); j++ {
			if hasBlock[j] {
				tmpIn[j] = <-inBufs[j]
			} else {
				tmpIn[j] = zeroPkt
			}
		}
		if needRepair {
			err := enc.Repair(tmpIn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Decode Repair Error: %s", err.Error())
			}
		}
		for j := 0; j < len(outBufs); j++ {
			outBufs[j] <- tmpIn[j]
		}
	}
	for i := 0; i < len(outBufs); i++ {
		close(outBufs[i])
	}
}

func (svc *ObjectService) send(node ramsg.RespNode, inBuf chan []byte, numPacket int64, wg *sync.WaitGroup, hashs []string, idx int) error {
	// TODO zkx 先直接复制client\internal\task\upload_rep_objects.go中的uploadToNode和uploadToLocalIPFS来替代这部分逻辑
	// 方便之后异步化处理
	// uploadToAgent的逻辑反了，而且中间步骤失败，就必须打印日志后停止后续操作

	uploadToAgent := true
	if svc.ipfs != nil { //使用IPFS传输
		//创建IPFS文件
		log.Infof("try to use local IPFS to upload block")
		writer, err := svc.ipfs.CreateFile()
		if err != nil {
			uploadToAgent = false
			fmt.Errorf("create IPFS file failed, err: %w", err)
		}
		//逐packet写进ipfs
		for i := 0; int64(i) < numPacket; i++ {
			buf := <-inBuf
			reader := bytes.NewReader(buf)
			_, err = io.Copy(writer, reader)
			if err != nil {
				uploadToAgent = false
				fmt.Errorf("copying block data to IPFS file failed, err: %w", err)
			}
		}
		//finish, 获取哈希
		fileHash, err := writer.Finish()
		if err != nil {
			log.Warnf("upload block to local IPFS failed, so try to upload by agent, err: %s", err.Error())
			uploadToAgent = false
			fmt.Errorf("finish writing blcok to IPFS failed, err: %w", err)
		}
		hashs[idx] = fileHash
		if err != nil {
		}
		nodeID := node.ID
		// 然后让最近节点pin本地上传的文件
		agentClient, err := agtcli.NewClient(nodeID, &config.Cfg().RabbitMQ)
		if err != nil {
			uploadToAgent = false
			fmt.Errorf("create agent client to %d failed, err: %w", nodeID, err)
		}
		defer agentClient.Close()

		pinObjResp, err := agentClient.StartPinningObject(agtmsg.NewStartPinningObject(fileHash))
		if err != nil {
			uploadToAgent = false
			fmt.Errorf("start pinning object: %w", err)
		}
		for {
			waitResp, err := agentClient.WaitPinningObject(agtmsg.NewWaitPinningObject(pinObjResp.TaskID, int64(time.Second)*5))
			if err != nil {
				uploadToAgent = false
				fmt.Errorf("waitting pinning object: %w", err)
			}
			if waitResp.IsComplete {
				if waitResp.Error != "" {
					uploadToAgent = false
					fmt.Errorf("agent pinning object: %s", waitResp.Error)
				}
				break
			}
		}
		if uploadToAgent == false {
			return nil
		}
	}
	//////////////////////////////通过Agent上传
	if uploadToAgent == true {
		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		nodeIP := node.ExternalIP
		if node.IsSameLocation {
			nodeIP = node.LocalIP

			log.Infof("client and node %d are at the same location, use local ip\n", node.ID)
		}

		grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
		grpcCon, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
		}
		defer grpcCon.Close()

		client := agentcaller.NewFileTransportClient(grpcCon)
		upload, err := mygrpc.SendFileAsStream(client)
		if err != nil {
			return fmt.Errorf("request to send file failed, err: %w", err)
		}
		// 发送文件数据
		for i := 0; int64(i) < numPacket; i++ {
			buf := <-inBuf
			reader := bytes.NewReader(buf)
			_, err = io.Copy(upload, reader)
			if err != nil {
				// 发生错误则关闭连接
				upload.Abort(io.ErrClosedPipe)
				return fmt.Errorf("copy block date to upload stream failed, err: %w", err)
			}
		}
		// 发送EOF消息，并获得FileHash
		fileHash, err := upload.Finish()
		if err != nil {
			upload.Abort(io.ErrClosedPipe)
			return fmt.Errorf("send EOF failed, err: %w", err)
		}
		hashs[idx] = fileHash
		wg.Done()
	}
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
