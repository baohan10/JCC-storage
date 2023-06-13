package cmd

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/agent/internal/task"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/common/utils"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/ec"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
)

func (service *Service) MoveObjectToStorage(msg *agtmsg.MoveObjectToStorage) *agtmsg.MoveObjectToStorageResp {
	outFileName := utils.MakeMoveOperationFileName(msg.Body.ObjectID, msg.Body.UserID)
	outFilePath := filepath.Join(config.Cfg().StorageBaseDir, msg.Body.Directory, outFileName)

	if msg.Body.Redundancy == consts.REDUNDANCY_REP {
		err := service.moveRepObject(msg, outFilePath)
		if err != nil {
			logger.Warnf("move rep object as %s failed, err: %s", outFilePath, err.Error())
			return ramsg.ReplyFailed[agtmsg.MoveObjectToStorageResp](errorcode.OPERATION_FAILED, "move rep object failed")
		}

	} else {
		return ramsg.ReplyFailed[agtmsg.MoveObjectToStorageResp](errorcode.OPERATION_FAILED, "not implement yet!")
	}

	return ramsg.ReplyOK(agtmsg.NewMoveObjectToStorageRespBody())
}

func (svc *Service) moveRepObject(msg *agtmsg.MoveObjectToStorage, outFilePath string) error {
	var repInfo ramsg.ObjectRepInfo
	err := serder.MapToObject(msg.Body.RedundancyData.(map[string]any), &repInfo)
	if err != nil {
		return fmt.Errorf("redundancy data to rep info failed, err: %w", err)
	}

	// TODO 可以考虑把整个API都做成异步的
	tsk := svc.taskManager.StartCmp(task.NewIPFSRead(repInfo.FileHash, outFilePath))
	tsk.Wait()

	if tsk.Error() != nil {
		return tsk.Error()
	}

	return nil
}

func (svc *Service) CheckStorage(msg *agtmsg.CheckStorage) *agtmsg.CheckStorageResp {
	dirFullPath := filepath.Join(config.Cfg().StorageBaseDir, msg.Body.Directory)

	infos, err := ioutil.ReadDir(dirFullPath)
	if err != nil {
		logger.Warnf("list storage directory failed, err: %s", err.Error())
		return ramsg.ReplyOK(agtmsg.NewCheckStorageRespBody(
			err.Error(),
			nil,
		))
	}

	fileInfos := lo.Filter(infos, func(info fs.FileInfo, index int) bool { return !info.IsDir() })

	if msg.Body.IsComplete {
		return svc.checkStorageComplete(msg, fileInfos)
	} else {
		return svc.checkStorageIncrement(msg, fileInfos)
	}
}

func (svc *Service) checkStorageIncrement(msg *agtmsg.CheckStorage, fileInfos []fs.FileInfo) *agtmsg.CheckStorageResp {
	infosMap := make(map[string]fs.FileInfo)
	for _, info := range fileInfos {
		infosMap[info.Name()] = info
	}

	var entries []agtmsg.CheckStorageRespEntry
	for _, obj := range msg.Body.Objects {
		fileName := utils.MakeMoveOperationFileName(obj.ObjectID, obj.UserID)
		_, ok := infosMap[fileName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, fileName)

		} else {
			// 只要文件不存在，就删除StorageObject表中的记录
			entries = append(entries, agtmsg.NewCheckStorageRespEntry(obj.ObjectID, obj.UserID, agtmsg.CHECK_STORAGE_RESP_OP_DELETE))
		}
	}

	// 增量情况下，不需要对infosMap中没检查的记录进行处理

	return ramsg.ReplyOK(agtmsg.NewCheckStorageRespBody(consts.STORAGE_DIRECTORY_STATE_OK, entries))
}

func (svc *Service) checkStorageComplete(msg *agtmsg.CheckStorage, fileInfos []fs.FileInfo) *agtmsg.CheckStorageResp {

	infosMap := make(map[string]fs.FileInfo)
	for _, info := range fileInfos {
		infosMap[info.Name()] = info
	}

	var entries []agtmsg.CheckStorageRespEntry
	for _, obj := range msg.Body.Objects {
		fileName := utils.MakeMoveOperationFileName(obj.ObjectID, obj.UserID)
		_, ok := infosMap[fileName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, fileName)

		} else {
			// 只要文件不存在，就删除StorageObject表中的记录
			entries = append(entries, agtmsg.NewCheckStorageRespEntry(obj.ObjectID, obj.UserID, agtmsg.CHECK_STORAGE_RESP_OP_DELETE))
		}
	}

	// Storage中多出来的文件不做处理

	return ramsg.ReplyOK(agtmsg.NewCheckStorageRespBody(consts.STORAGE_DIRECTORY_STATE_OK, entries))
}

/*
func (service *Service) ECMove(msg *agtmsg.ECMoveCommand) *agtmsg.MoveObjectToStorageResp {
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
