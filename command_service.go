package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"gitlink.org.cn/cloudream/agent/config"
	"gitlink.org.cn/cloudream/ec"
	"gitlink.org.cn/cloudream/utils"

	racli "gitlink.org.cn/cloudream/rabbitmq/client"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
)

type CommandService struct {
}

func (service *CommandService) RepMove(msg *ramsg.RepMoveCommand) ramsg.AgentMoveResp {
	/*fmt.Println("RepMove")
	fmt.Println(command.Hashs)
	hashs := command.Hashs
	//执行调度操作
	ipfsDir := "assets"
	goalDir := "assets2"
	goalName := command.BucketName + ":" + command.ObjectName + ":" + strconv.Itoa(command.UserId)
	//目标文件
	fDir, err := os.Executable()
	if err != nil {
		panic(err)
	}
	fURL := filepath.Join(filepath.Dir(fDir), goalDir)

	_, err = os.Stat(fURL)
	if os.IsNotExist(err) {
		os.MkdirAll(fURL, os.ModePerm)
	}
	fURL = filepath.Join(fURL, goalName)
	outFile, err := os.Create(fURL)

	fmt.Println(fURL)
	//源文件
	fURL = filepath.Join(filepath.Dir(fDir), ipfsDir)
	fURL = filepath.Join(fURL, hashs[0])
	inFile, _ := os.Open(fURL)
	fmt.Println(fURL)
	fileInfo, _ := inFile.Stat()
	fileSizeInBytes := fileInfo.Size()
	numWholePacket := fileSizeInBytes / config.Cfg().GRCPPacketSize
	lastPacketInBytes := fileSizeInBytes % config.Cfg().GRCPPacketSize
	fmt.Println(fileSizeInBytes)
	fmt.Println(numWholePacket)
	fmt.Println(lastPacketInBytes)
	for i := 0; int64(i) < numWholePacket; i++ {
		buf := make([]byte, config.Cfg().GRCPPacketSize)
		inFile.Read(buf)
		outFile.Write(buf)
	}
	if lastPacketInBytes > 0 {
		buf := make([]byte, lastPacketInBytes)
		inFile.Read(buf)
		outFile.Write(buf)
	}
	inFile.Close()
	outFile.Close()
	//返回消息
	res := rabbitmq.AgentMoveRes{
		MoveCode: 0,
	}
	c, _ := json.Marshal(res)
	rabbitSend(c, command.UserId)
	//向coor报告临时缓存hash
	command1 := rabbitmq.TempCacheReport{
		Ip:    LocalIp,
		Hashs: hashs,
	}
	c, _ = json.Marshal(command1)
	b := append([]byte("06"), c...)
	fmt.Println(b)
	rabbit := rabbitmq.NewRabbitMQSimple("coorQueue")
	rabbit.PublishSimple(b)
	rabbit.Destroy()*/
	fmt.Println("RepMove")
	fmt.Println(msg.Hashs)
	hashs := msg.Hashs
	fileSizeInBytes := msg.FileSizeInBytes
	//执行调度操作
	goalDir := "assets2"
	goalName := msg.BucketName + ":" + msg.ObjectName + ":" + strconv.Itoa(msg.UserID)
	//目标文件
	fDir, err := os.Executable()
	if err != nil {
		panic(err)
	}
	fURL := filepath.Join(filepath.Dir(fDir), goalDir)

	_, err = os.Stat(fURL)
	if os.IsNotExist(err) {
		os.MkdirAll(fURL, os.ModePerm)
	}
	fURL = filepath.Join(fURL, goalName)
	outFile, err := os.Create(fURL)

	fmt.Println(fURL)
	//源文件
	data := CatIPFS(hashs[0])

	numWholePacket := fileSizeInBytes / int64(config.Cfg().GRCPPacketSize)
	lastPacketInBytes := fileSizeInBytes % int64(config.Cfg().GRCPPacketSize)
	fmt.Println(fileSizeInBytes)
	fmt.Println(numWholePacket)
	fmt.Println(lastPacketInBytes)
	for i := 0; int64(i) < numWholePacket; i++ {
		buf := []byte(data[i*config.Cfg().GRCPPacketSize : i*config.Cfg().GRCPPacketSize+config.Cfg().GRCPPacketSize])
		outFile.Write(buf)
	}
	if lastPacketInBytes > 0 {
		buf := []byte(data[numWholePacket*int64(config.Cfg().GRCPPacketSize) : numWholePacket*int64(config.Cfg().GRCPPacketSize)+lastPacketInBytes])
		outFile.Write(buf)
	}
	outFile.Close()

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
	for i := 0; i < len(blockIds); i++ {
		go get(hashs[i], getBufs[blockIds[i]], numPacket)
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

func get(blockHash string, getBuf chan []byte, numPacket int64) {
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
