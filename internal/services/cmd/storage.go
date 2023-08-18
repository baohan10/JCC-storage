package cmd

import (
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage-agent/internal/config"
	mytask "gitlink.org.cn/cloudream/storage-agent/internal/task"
	"gitlink.org.cn/cloudream/storage-common/consts"
	"gitlink.org.cn/cloudream/storage-common/utils"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgcmd "gitlink.org.cn/cloudream/storage-common/pkgs/cmd"
	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
	myos "gitlink.org.cn/cloudream/storage-common/utils/os"
)

func (svc *Service) StartStorageMovePackage(msg *agtmq.StartStorageMovePackage) (*agtmq.StartStorageMovePackageResp, *mq.CodeMessage) {
	getStgResp, err := svc.coordinator.GetStorageInfo(coormq.NewGetStorageInfo(msg.UserID, msg.StorageID))
	if err != nil {
		logger.WithField("StorageID", msg.StorageID).
			Warnf("getting storage info: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get storage info failed")
	}

	outputDirPath := filepath.Join(config.Cfg().StorageBaseDir, getStgResp.Directory, utils.MakeStorageMovePackageDirName(msg.PackageID, msg.UserID))
	if err = os.MkdirAll(outputDirPath, 0755); err != nil {
		logger.WithField("StorageID", msg.StorageID).
			Warnf("creating output directory: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "create output directory failed")
	}

	tsk := svc.taskManager.StartNew(stgcmd.Wrap[mytask.TaskContext](stgcmd.NewDownloadPackage(msg.UserID, msg.PackageID, outputDirPath)))
	return mq.ReplyOK(agtmq.NewStartStorageMovePackageResp(tsk.ID()))
}

func (svc *Service) WaitStorageMovePackage(msg *agtmq.WaitStorageMovePackage) (*agtmq.WaitStorageMovePackageResp, *mq.CodeMessage) {
	logger.WithField("TaskID", msg.TaskID).Debugf("wait moving package")

	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return mq.ReplyFailed[agtmq.WaitStorageMovePackageResp](errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()

		errMsg := ""
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error()
		}

		return mq.ReplyOK(agtmq.NewWaitStorageMovePackageResp(true, errMsg))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs)) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			return mq.ReplyOK(agtmq.NewWaitStorageMovePackageResp(true, errMsg))
		}

		return mq.ReplyOK(agtmq.NewWaitStorageMovePackageResp(false, ""))
	}
}

func (svc *Service) StorageCheck(msg *agtmq.StorageCheck) (*agtmq.StorageCheckResp, *mq.CodeMessage) {
	dirFullPath := filepath.Join(config.Cfg().StorageBaseDir, msg.Directory)

	infos, err := ioutil.ReadDir(dirFullPath)
	if err != nil {
		logger.Warnf("list storage directory failed, err: %s", err.Error())
		return mq.ReplyOK(agtmq.NewStorageCheckResp(
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

func (svc *Service) checkStorageIncrement(msg *agtmq.StorageCheck, fileInfos []fs.FileInfo) (*agtmq.StorageCheckResp, *mq.CodeMessage) {
	infosMap := make(map[string]fs.FileInfo)
	for _, info := range fileInfos {
		infosMap[info.Name()] = info
	}

	var entries []agtmq.StorageCheckRespEntry
	for _, obj := range msg.Packages {
		fileName := utils.MakeStorageMovePackageDirName(obj.PackageID, obj.UserID)
		_, ok := infosMap[fileName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, fileName)

		} else {
			// 只要文件不存在，就删除StoragePackage表中的记录
			entries = append(entries, agtmq.NewStorageCheckRespEntry(obj.PackageID, obj.UserID, agtmq.CHECK_STORAGE_RESP_OP_DELETE))
		}
	}

	// 增量情况下，不需要对infosMap中没检查的记录进行处理

	return mq.ReplyOK(agtmq.NewStorageCheckResp(consts.StorageDirectoryStateOK, entries))
}

func (svc *Service) checkStorageComplete(msg *agtmq.StorageCheck, fileInfos []fs.FileInfo) (*agtmq.StorageCheckResp, *mq.CodeMessage) {

	infosMap := make(map[string]fs.FileInfo)
	for _, info := range fileInfos {
		infosMap[info.Name()] = info
	}

	var entries []agtmq.StorageCheckRespEntry
	for _, obj := range msg.Packages {
		fileName := utils.MakeStorageMovePackageDirName(obj.PackageID, obj.UserID)
		_, ok := infosMap[fileName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, fileName)

		} else {
			// 只要文件不存在，就删除StoragePackage表中的记录
			entries = append(entries, agtmq.NewStorageCheckRespEntry(obj.PackageID, obj.UserID, agtmq.CHECK_STORAGE_RESP_OP_DELETE))
		}
	}

	return mq.ReplyOK(agtmq.NewStorageCheckResp(consts.StorageDirectoryStateOK, entries))
}

func (svc *Service) StartStorageCreatePackage(msg *agtmq.StartStorageCreatePackage) (*agtmq.StartStorageCreatePackageResp, *mq.CodeMessage) {
	getStgResp, err := svc.coordinator.GetStorageInfo(coormq.NewGetStorageInfo(msg.UserID, msg.StorageID))
	if err != nil {
		logger.WithField("StorageID", msg.StorageID).
			Warnf("getting storage info: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get storage info failed")
	}

	fullPath := filepath.Join(config.Cfg().StorageBaseDir, getStgResp.Directory, msg.Path)

	var uploadFilePathes []string
	err = filepath.WalkDir(fullPath, func(fname string, fi os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if !fi.IsDir() {
			uploadFilePathes = append(uploadFilePathes, fname)
		}

		return nil
	})
	if err != nil {
		logger.Warnf("opening directory %s: %s", fullPath, err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "read directory failed")
	}

	objIter := myos.NewUploadingObjectIterator(fullPath, uploadFilePathes)

	if msg.Redundancy.Type == models.RedundancyRep {
		repInfo, err := msg.Redundancy.ToRepInfo()
		if err != nil {
			logger.Warnf("getting rep redundancy info: %s", err.Error())

			return nil, mq.Failed(errorcode.OperationFailed, "get rep redundancy info failed")
		}

		tsk := svc.taskManager.StartNew(stgcmd.Wrap[mytask.TaskContext](
			stgcmd.NewCreateRepPackage(msg.UserID, msg.BucketID, msg.Name, objIter, repInfo, stgcmd.UploadConfig{
				LocalIPFS:   svc.ipfs,
				LocalNodeID: &config.Cfg().ID,
				ExternalIP:  config.Cfg().ExternalIP,
				GRPCPort:    config.Cfg().GRPCPort,
				MQ:          &config.Cfg().RabbitMQ,
			})))
		return mq.ReplyOK(agtmq.NewStartStorageCreatePackageResp(tsk.ID()))
	}

	ecInfo, err := msg.Redundancy.ToECInfo()
	if err != nil {
		logger.Warnf("getting ec redundancy info: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get ec redundancy info failed")
	}

	tsk := svc.taskManager.StartNew(stgcmd.Wrap[mytask.TaskContext](
		stgcmd.NewCreateECPackage(msg.UserID, msg.BucketID, msg.Name, objIter, ecInfo, config.Cfg().ECPacketSize, stgcmd.UploadConfig{
			LocalIPFS:   svc.ipfs,
			LocalNodeID: &config.Cfg().ID,
			ExternalIP:  config.Cfg().ExternalIP,
			GRPCPort:    config.Cfg().GRPCPort,
			MQ:          &config.Cfg().RabbitMQ,
		})))
	return mq.ReplyOK(agtmq.NewStartStorageCreatePackageResp(tsk.ID()))
}

func (svc *Service) WaitStorageCreatePackage(msg *agtmq.WaitStorageCreatePackage) (*agtmq.WaitStorageCreatePackageResp, *mq.CodeMessage) {
	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return nil, mq.Failed(errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()
	} else if !tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs)) {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(false, "", 0))
	}

	wrapTask := tsk.Body().(*stgcmd.TaskWrapper[mytask.TaskContext])

	if tsk.Error() != nil {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, tsk.Error().Error(), 0))
	}

	if repTask, ok := wrapTask.InnerTask().(*stgcmd.CreateRepPackage); ok {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, "", repTask.Result.PackageID))
	}

	if ecTask, ok := wrapTask.InnerTask().(*stgcmd.CreateECPackage); ok {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, "", ecTask.Result.PackageID))
	}

	return nil, mq.Failed(errorcode.TaskNotFound, "task not found")
}
