package cmd

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/agent/internal/task"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/common/utils"
	"gitlink.org.cn/cloudream/ec"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
)

func (service *Service) StartStorageMoveObject(msg *agtmsg.StartStorageMoveObject) (*agtmsg.StartStorageMoveObjectResp, *ramsg.CodeMessage) {
	// TODO  修改文件名，可用objectname
	outFileName := utils.MakeMoveOperationFileName(msg.ObjectID, msg.UserID)
	outFilePath := filepath.Join(config.Cfg().StorageBaseDir, msg.Directory, outFileName)

	if repRed, ok := msg.Redundancy.(models.RepRedundancyData); ok {
		taskID, err := service.moveRepObject(repRed, outFilePath)
		if err != nil {
			logger.Warnf("move rep object as %s failed, err: %s", outFilePath, err.Error())
			return ramsg.ReplyFailed[agtmsg.StartStorageMoveObjectResp](errorcode.OperationFailed, "move rep object failed")
		}

		return ramsg.ReplyOK(agtmsg.NewStartStorageMoveObjectResp(taskID))

	} else {
		// TODO 处理其他备份类型

		return ramsg.ReplyFailed[agtmsg.StartStorageMoveObjectResp](errorcode.OperationFailed, "not implement yet!")
	}
}

func (svc *Service) moveRepObject(repData models.RepRedundancyData, outFilePath string) (string, error) {
	tsk := svc.taskManager.StartComparable(task.NewIPFSRead(repData.FileHash, outFilePath))
	return tsk.ID(), nil
}

func (svc *Service) WaitStorageMoveObject(msg *agtmsg.WaitStorageMoveObject) (*agtmsg.WaitStorageMoveObjectResp, *ramsg.CodeMessage) {
	logger.WithField("TaskID", msg.TaskID).Debugf("wait moving object")

	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return ramsg.ReplyFailed[agtmsg.WaitStorageMoveObjectResp](errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()

		errMsg := ""
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error()
		}

		return ramsg.ReplyOK(agtmsg.NewWaitStorageMoveObjectResp(true, errMsg))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs)) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			return ramsg.ReplyOK(agtmsg.NewWaitStorageMoveObjectResp(true, errMsg))
		}

		return ramsg.ReplyOK(agtmsg.NewWaitStorageMoveObjectResp(false, ""))
	}
}

func (svc *Service) StorageCheck(msg *agtmsg.StorageCheck) (*agtmsg.StorageCheckResp, *ramsg.CodeMessage) {
	dirFullPath := filepath.Join(config.Cfg().StorageBaseDir, msg.Directory)

	infos, err := ioutil.ReadDir(dirFullPath)
	if err != nil {
		logger.Warnf("list storage directory failed, err: %s", err.Error())
		return ramsg.ReplyOK(agtmsg.NewStorageCheckResp(
			err.Error(),
			nil,
		))
	}

	fileInfos := lo.Filter(infos, func(info fs.FileInfo, index int) bool { return !info.IsDir() })

	if msg.IsComplete {
		return svc.checkStorageComplete(msg, fileInfos)
	} else {
		return svc.checkStorageIncrement(msg, fileInfos)
	}
}

func (svc *Service) checkStorageIncrement(msg *agtmsg.StorageCheck, fileInfos []fs.FileInfo) (*agtmsg.StorageCheckResp, *ramsg.CodeMessage) {
	infosMap := make(map[string]fs.FileInfo)
	for _, info := range fileInfos {
		infosMap[info.Name()] = info
	}

	var entries []agtmsg.StorageCheckRespEntry
	for _, obj := range msg.Objects {
		fileName := utils.MakeMoveOperationFileName(obj.ObjectID, obj.UserID)
		_, ok := infosMap[fileName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, fileName)

		} else {
			// 只要文件不存在，就删除StorageObject表中的记录
			entries = append(entries, agtmsg.NewStorageCheckRespEntry(obj.ObjectID, obj.UserID, agtmsg.CHECK_STORAGE_RESP_OP_DELETE))
		}
	}

	// 增量情况下，不需要对infosMap中没检查的记录进行处理

	return ramsg.ReplyOK(agtmsg.NewStorageCheckResp(consts.StorageDirectoryStateOK, entries))
}

func (svc *Service) checkStorageComplete(msg *agtmsg.StorageCheck, fileInfos []fs.FileInfo) (*agtmsg.StorageCheckResp, *ramsg.CodeMessage) {

	infosMap := make(map[string]fs.FileInfo)
	for _, info := range fileInfos {
		infosMap[info.Name()] = info
	}

	var entries []agtmsg.StorageCheckRespEntry
	for _, obj := range msg.Objects {
		fileName := utils.MakeMoveOperationFileName(obj.ObjectID, obj.UserID)
		_, ok := infosMap[fileName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, fileName)

		} else {
			// 只要文件不存在，就删除StorageObject表中的记录
			entries = append(entries, agtmsg.NewStorageCheckRespEntry(obj.ObjectID, obj.UserID, agtmsg.CHECK_STORAGE_RESP_OP_DELETE))
		}
	}

	// Storage中多出来的文件不做处理

	return ramsg.ReplyOK(agtmsg.NewStorageCheckResp(consts.StorageDirectoryStateOK, entries))
}

/*
func (service *Service) ECMove(msg *agtmsg.ECMoveCommand) *agtmsg.StartStorageMoveObjectResp {
	panic("not implement yet!")
		wg := sync.WaitGroup{}
		fmt.Println("EcMove")
		fmt.Println(msg.Hashs)
		hashs := msg.Hashs
		fileSize := msg.FileSize
		blockIds := msg.IDs
		ecName := msg.ECName
		goalName := msg.BucketName + ":" + msg.ObjectName + ":" + strconv.Itoa(msg.UserID)
		ecPolicies := *utils.GetEcPolicy()
		ecPolicy := ecPolicies[ecName]
		ecK := ecPolicy.GetK()
		ecN := ecPolicy.GetN()
		numPacket := (fileSize + int64(ecK)*int64(config.Cfg().GRCPPacketSize) - 1) / (int64(ecK) * int64(config.Cfg().GRCPPacketSize))

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
		// TODO 写入的文件路径需要带上msg中的Directory字段，参考RepMove
		go persist(decodeBufs[:], numPacket, goalName, &wg)
		wg.Wait()

		//向coor报告临时缓存hash
		coorClient, err := racli.NewCoordinatorClient()
		if err != nil {
			// TODO 日志
			return ramsg.NewAgentMoveRespFailed(errorcode.OPERATION_FAILED, fmt.Sprintf("create coordinator client failed"))
		}
		defer coorClient.Close()
		coorClient.TempCacheReport(NodeID, hashs)

		return ramsg.NewAgentMoveRespOK()
}
*/

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

func (service *Service) get(blockHash string, getBuf chan []byte, numPacket int64) {
	/*
		data := CatIPFS(blockHash)
		for i := 0; int64(i) < numPacket; i++ {
			buf := []byte(data[i*config.Cfg().GRCPPacketSize : i*config.Cfg().GRCPPacketSize+config.Cfg().GRCPPacketSize])
			getBuf <- buf
		}
		close(getBuf)
	*/
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

func (svc *Service) StartStorageUploadRepObject(msg *agtmsg.StartStorageUploadRepObject) (*agtmsg.StartStorageUploadRepObjectResp, *ramsg.CodeMessage) {
	fullPath := filepath.Join(config.Cfg().StorageBaseDir, msg.StorageDirectory, msg.FilePath)

	file, err := os.Open(fullPath)
	if err != nil {
		logger.Warnf("opening file %s: %s", fullPath, err.Error())
		return nil, ramsg.Failed(errorcode.OperationFailed, "open file failed")
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		logger.Warnf("getting file %s state: %s", fullPath, err.Error())
		return nil, ramsg.Failed(errorcode.OperationFailed, "get file info failed")
	}
	fileSize := fileInfo.Size()

	uploadObject := task.UploadObject{
		ObjectName: msg.ObjectName,
		File:       file,
		FileSize:   fileSize,
	}
	uploadObjects := []task.UploadObject{uploadObject}

	// Task会关闭文件流
	tsk := svc.taskManager.StartNew(task.NewUploadRepObjects(msg.UserID, msg.BucketID, uploadObjects, msg.RepCount))
	return ramsg.ReplyOK(agtmsg.NewStartStorageUploadRepObjectResp(tsk.ID()))
}

func (svc *Service) WaitStorageUploadRepObject(msg *agtmsg.WaitStorageUploadRepObject) (*agtmsg.WaitStorageUploadRepObjectResp, *ramsg.CodeMessage) {
	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return nil, ramsg.Failed(errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()
	} else if !tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs)) {
		return ramsg.ReplyOK(agtmsg.NewWaitStorageUploadRepObjectResp(false, "", 0, ""))
	}

	uploadTask := tsk.Body().(*task.UploadRepObjects)
	uploadRet := uploadTask.Results[0]

	errMsg := ""
	if tsk.Error() != nil {
		errMsg = tsk.Error().Error()
	}

	if uploadRet.Error != nil {
		errMsg = uploadRet.Error.Error()
	}

	return ramsg.ReplyOK(agtmsg.NewWaitStorageUploadRepObjectResp(true, errMsg, uploadRet.ObjectID, uploadRet.FileHash))
}
