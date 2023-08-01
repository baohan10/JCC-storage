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

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/common/utils"
	"gitlink.org.cn/cloudream/ec"

	//"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
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
	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于判断用户是否有桶的权限
		UserBucket().ReadOne(userID, bucketID).
		// 用于防止创建了多个同名对象
		Object().CreateOne(bucketID, objectName).
		// 用于查询可用的上传节点
		Node().ReadAny().
		// 用于设置Block配置
		ObjectBlock().CreateAny().
		// 用于创建Cache记录
		Cache().CreateAny().
		MutexLock(svc.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	//发送写请求，请求Coor分配写入节点Ip
	ecWriteResp, err := svc.coordinator.PreUploadEcObject(coormsg.NewPreUploadEcObject(bucketID, objectName, fileSize, ecName, userID, config.Cfg().ExternalIP))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}

	if len(ecWriteResp.Nodes) == 0 {
		return fmt.Errorf("no node to upload file")
	}
	//fmt.Println(ecWriteResp)
	print(ecWriteResp.Ec.EcK)
	print(ecWriteResp.Ec.EcN)
	//fmt.Println(ecWriteResp.Body.Nodes)
	//生成纠删码的写入节点序列
	nds := make([]ramsg.RespNode, ecWriteResp.Ec.EcN)
	ll := len(ecWriteResp.Nodes)
	start := rand.Intn(ll)
	for i := 0; i < ecWriteResp.Ec.EcN; i++ {
		nds[i] = ecWriteResp.Nodes[(start+i)%ll]
	}
	hashs, err := svc.EcWrite(file, fileSize, ecWriteResp.Ec.EcK, ecWriteResp.Ec.EcN, nds)
	if err != nil {
		return fmt.Errorf("EcWrite failed, err: %w", err)
	}
	nodeIDs := make([]int64, len(nds))
	for i := 0; i < len(nds); i++ {
		nodeIDs[i] = nds[i].ID
	}
	//第二轮通讯:插入元数据hashs
	_, err = svc.coordinator.CreateEcObject(coormsg.NewCreateEcObject(bucketID, objectName, fileSize, userID, nodeIDs, hashs, ecName))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	return nil
}

func (svc *ObjectService) EcWrite(file io.ReadCloser, fileSize int64, ecK int, ecN int, nodes []ramsg.RespNode) ([]string, error) {

	// TODO 需要参考RepWrite函数的代码逻辑，做好错误处理
	//获取文件大小

	var coefs = [][]int64{{1, 1, 1}, {1, 2, 3}} //2应替换为ecK，3应替换为ecN
	//计算每个块的packet数
	numPacket := (fileSize + int64(ecK)*config.Cfg().GRPCPacketSize - 1) / (int64(ecK) * config.Cfg().GRPCPacketSize)
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
		go svc.Send(nodes[i], encodeBufs[i], numPacket, &wg, hashs, i)
	}
	wg.Wait()

	return hashs, nil

}

func load(file io.ReadCloser, loadBufs []chan []byte, ecK int, totalNumPacket int64) error {

	for i := 0; int64(i) < totalNumPacket; i++ {

		buf := make([]byte, config.Cfg().GRPCPacketSize)
		idx := i % ecK
		_, err := file.Read(buf)
		if err != nil {
			return fmt.Errorf("read file falied, err:%w", err)
		}
		loadBufs[idx] <- buf

		if idx == ecK-1 {
			//print("***")
			for j := ecK; j < len(loadBufs); j++ {
				zeroPkt := make([]byte, config.Cfg().GRPCPacketSize)
				loadBufs[j] <- zeroPkt
			}
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("load file to buf failed, err:%w", err)
		}
	}
	//fmt.Println("load over")
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
		for j := 0; j < len(outBufs); j++ { //3
			tmpIn[j] = <-inBufs[j]
		}
		enc.Encode(tmpIn)
		for j := 0; j < len(outBufs); j++ { //1,2,3//示意，需要调用纠删码编解码引擎：  tmp[k] = tmp[k]+(tmpIn[w][k]*coefs[w][j])
			outBufs[j] <- tmpIn[j]
		}
	}
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

func (svc *ObjectService) Send(node ramsg.RespNode, inBuf chan []byte, numPacket int64, wg *sync.WaitGroup, hashs []string, idx int) error {
	uploadToAgent := true
	if svc.ipfs != nil { //使用IPFS传输
		//创建IPFS文件
		print("!!!")
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

		pinObjResp, err := agentClient.PinObject(agtmsg.NewPinObjectBody(fileHash))
		if err != nil {
			uploadToAgent = false
			fmt.Errorf("request to agent %d failed, err: %w", nodeID, err)
		}
		if pinObjResp.IsFailed() {
			uploadToAgent = false
			fmt.Errorf("agent %d PinObject failed, code: %s, message: %s", nodeID, pinObjResp.ErrorCode, pinObjResp.ErrorMessage)
		}
		if uploadToAgent == false {
			return nil
		}
	}
	//////////////////////////////通过Agent上传
	if uploadToAgent == true {
		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		print("!!!!!!")
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

func ecRead(fileSize int64, nodeIPs []string, blockHashs []string, blockIds []int, ecName string, localFilePath string) {
	//根据ecName获得以下参数
	wg := sync.WaitGroup{}
	ecPolicies := *utils.GetEcPolicy()
	ecPolicy := ecPolicies[ecName]
	fmt.Println(ecPolicy)
	ecK := ecPolicy.GetK()
	ecN := ecPolicy.GetN()
	var coefs = [][]int64{{1, 1, 1}, {1, 2, 3}} //2应替换为ecK，3应替换为ecN

	numPacket := (fileSize + int64(ecK)*config.Cfg().GRPCPacketSize - 1) / (int64(ecK) * config.Cfg().GRPCPacketSize)
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
