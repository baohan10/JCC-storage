package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"
	"gitlink.org.cn/cloudream/agent/config"
	"gitlink.org.cn/cloudream/ec"
	"gitlink.org.cn/cloudream/utils"

	racli "gitlink.org.cn/cloudream/rabbitmq/client"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
	myio "gitlink.org.cn/cloudream/utils/io"
	"gitlink.org.cn/cloudream/utils/ipfs"
)

type CommandService struct {
	ipfs *ipfs.IPFS
}

func NewCommandService(ipfs *ipfs.IPFS) *CommandService {
	return &CommandService{
		ipfs: ipfs,
	}
}

func (service *CommandService) RepMove(msg *ramsg.RepMoveCommand) ramsg.AgentMoveResp {
	hashs := msg.Hashs
	//执行调度操作
	//TODO xh: 调度到BackID对应的dir中，即goalDir改为传过来的agentMoveResp.dir
	goalDir := "assets2"
	goalName := msg.BucketName + ":" + msg.ObjectName + ":" + strconv.Itoa(msg.UserID)
	//目标文件
	fDir, err := os.Executable()
	if err != nil {
		panic(err)
	}
	fURL := filepath.Join(filepath.Dir(fDir), goalDir)
	// TODO2 这一块代码需要优化
	_, err = os.Stat(fURL)
	if os.IsNotExist(err) {
		os.MkdirAll(fURL, os.ModePerm)
	}
	fURL = filepath.Join(fURL, goalName)

	outFile, err := os.Create(fURL)
	if err != nil {
		log.Warnf("create file %s failed, err: %s", fURL, err.Error())
		return ramsg.NewAgentMoveRespFailed(errorcode.OPERATION_FAILED, fmt.Sprintf("create local file failed"))
	}
	defer outFile.Close()

	fileHash := hashs[0]
	ipfsRd, err := service.ipfs.OpenRead(fileHash)
	if err != nil {
		log.Warnf("read ipfs file %s failed, err: %s", fileHash, err.Error())
		return ramsg.NewAgentMoveRespFailed(errorcode.OPERATION_FAILED, fmt.Sprintf("read ipfs file failed"))
	}

	buf := make([]byte, 1024)
	for {
		readCnt, err := ipfsRd.Read(buf)

		// 文件读取完毕
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Warnf("read ipfs file %s data failed, err: %s", fileHash, err.Error())
			return ramsg.NewAgentMoveRespFailed(errorcode.OPERATION_FAILED, fmt.Sprintf("read ipfs file data failed"))
		}

		err = myio.WriteAll(outFile, buf[:readCnt])
		if err != nil {
			log.Warnf("write data to file %s failed, err: %s", fURL, err.Error())
			return ramsg.NewAgentMoveRespFailed(errorcode.OPERATION_FAILED, fmt.Sprintf("write data to file failed"))
		}
	}

	//向coor报告临时缓存hash
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		// TODO 日志
		return ramsg.NewAgentMoveRespFailed(errorcode.OPERATION_FAILED, fmt.Sprintf("create coordinator client failed"))
	}
	defer coorClient.Close()
	coorClient.TempCacheReport(config.Cfg().LocalIP, hashs)

	return ramsg.NewAgentMoveRespOK()
}

func (service *CommandService) ECMove(msg *ramsg.ECMoveCommand) ramsg.AgentMoveResp {
	wg := sync.WaitGroup{}
	fmt.Println("EcMove")
	fmt.Println(msg.Hashs)
	hashs := msg.Hashs
	fileSizeInBytes := msg.FileSizeInBytes
	blockIds := msg.IDs
	ecName := msg.ECName
	goalName := msg.BucketName + ":" + msg.ObjectName + ":" + strconv.Itoa(msg.UserID)
	ecPolicies := *utils.GetEcPolicy()
	ecPolicy := ecPolicies[ecName]
	ecK := ecPolicy.GetK()
	ecN := ecPolicy.GetN()
	numPacket := (fileSizeInBytes + int64(ecK)*int64(config.Cfg().GRCPPacketSize) - 1) / (int64(ecK) * int64(config.Cfg().GRCPPacketSize))

	getBufs := make([]chan []byte, ecN)
	decodeBufs := make([]chan []byte, ecK)
	for i := 0; i < ecN; i++ {
		getBufs[i] = make(chan []byte)
	}
	for i := 0; i < ecK; i++ {
		decodeBufs[i] = make(chan []byte)
	}

	wg.Add(1)

	//执行调度操作
	// TODO 这一块需要改写以适配IPFS流式读取
	for i := 0; i < len(blockIds); i++ {
		go service.get(hashs[i], getBufs[blockIds[i]], numPacket)
	}
	go decode(getBufs[:], decodeBufs[:], blockIds, ecK, numPacket)
	go persist(decodeBufs[:], numPacket, goalName, &wg)
	wg.Wait()

	//向coor报告临时缓存hash
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		// TODO 日志
		return ramsg.NewAgentMoveRespFailed(errorcode.OPERATION_FAILED, fmt.Sprintf("create coordinator client failed"))
	}
	defer coorClient.Close()
	coorClient.TempCacheReport(config.Cfg().LocalIP, hashs)

	return ramsg.NewAgentMoveRespOK()
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

func (service *CommandService) get(blockHash string, getBuf chan []byte, numPacket int64) {
	data := CatIPFS(blockHash)
	for i := 0; int64(i) < numPacket; i++ {
		buf := []byte(data[i*config.Cfg().GRCPPacketSize : i*config.Cfg().GRCPPacketSize+config.Cfg().GRCPPacketSize])
		getBuf <- buf
	}
	close(getBuf)
}

func persist(inBuf []chan []byte, numPacket int64, localFilePath string, wg *sync.WaitGroup) {
	//这里的localFilePath应该是要写入的filename
	fDir, err := os.Executable()
	if err != nil {
		panic(err)
	}
	fURL := filepath.Join(filepath.Dir(fDir), "assets3")
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
