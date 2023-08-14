package cmd

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/common/utils"
	"gitlink.org.cn/cloudream/ec"
	"gitlink.org.cn/cloudream/storage-agent/internal/config"
	"gitlink.org.cn/cloudream/storage-agent/internal/task"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
)

func (service *Service) StartStorageMoveObject(msg *agtmsg.StartStorageMoveObject) (*agtmsg.StartStorageMoveObjectResp, *ramsg.CodeMessage) {
	// TODO  修改文件名，可用objectname
	outFileName := utils.MakeMoveOperationFileName(msg.ObjectID, msg.UserID)
	objectDir := filepath.Dir(msg.ObjectName)
	outFilePath := filepath.Join(config.Cfg().StorageBaseDir, msg.Directory, objectDir, outFileName)

	if repRed, ok := msg.Redundancy.(models.RepRedundancyData); ok {
		taskID, err := service.moveRepObject(repRed, outFilePath)
		if err != nil {
			logger.Warnf("move rep object as %s failed, err: %s", outFilePath, err.Error())
			return ramsg.ReplyFailed[agtmsg.StartStorageMoveObjectResp](errorcode.OperationFailed, "move rep object failed")
		}

		return ramsg.ReplyOK(agtmsg.NewStartStorageMoveObjectResp(taskID))

	} else if repRed, ok := msg.Redundancy.(models.ECRedundancyData); ok {
		taskID, err := service.moveEcObject(msg.ObjectID, msg.FileSize, repRed, outFilePath)
		if err != nil {
			logger.Warnf("move ec object as %s failed, err: %s", outFilePath, err.Error())
			return ramsg.ReplyFailed[agtmsg.StartStorageMoveObjectResp](errorcode.OperationFailed, "move ec object failed")
		}

		return ramsg.ReplyOK(agtmsg.NewStartStorageMoveObjectResp(taskID))
	}

	return ramsg.ReplyFailed[agtmsg.StartStorageMoveObjectResp](errorcode.OperationFailed, "not rep or ec object???")
}

func (svc *Service) moveRepObject(repData models.RepRedundancyData, outFilePath string) (string, error) {
	tsk := svc.taskManager.StartComparable(task.NewIPFSRead(repData.FileHash, outFilePath))
	return tsk.ID(), nil
}

func (svc *Service) moveEcObject(objID int64, fileSize int64, ecData models.ECRedundancyData, outFilePath string) (string, error) {
	ecK := ecData.Ec.EcK
	blockIDs := make([]int, ecK)
	hashs := make([]string, ecK)
	for i := 0; i < ecK; i++ {
		blockIDs[i] = i
		hashs[i] = ecData.Blocks[i].FileHash
	}
	tsk := svc.taskManager.StartComparable(task.NewEcRead(objID, fileSize, ecData.Ec, blockIDs, hashs, outFilePath))
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

	return ramsg.ReplyOK(agtmsg.NewStorageCheckResp(consts.StorageDirectoryStateOK, entries))
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
		if needRepair {
			err := enc.Repair(tmpIn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Decode Repair Error: %s", err.Error())
			}
		}
		for j := 0; j < len(outBufs); j++ { //1,2,3//示意，需要调用纠删码编解码引擎：  tmp[k] = tmp[k]+(tmpIn[w][k]*coefs[w][j])
			outBufs[j] <- tmpIn[j]
		}
	}
	for i := 0; i < len(outBufs); i++ {
		close(outBufs[i])
	}
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
