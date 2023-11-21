package mq

import (
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mytask "gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/utils"
)

func (svc *Service) StartStorageLoadPackage(msg *agtmq.StartStorageLoadPackage) (*agtmq.StartStorageLoadPackageResp, *mq.CodeMessage) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		logger.Warnf("new coordinator client: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "new coordinator client failed")
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getStgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(msg.UserID, msg.StorageID))
	if err != nil {
		logger.WithField("StorageID", msg.StorageID).
			Warnf("getting storage info: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get storage info failed")
	}

	outputDirPath := utils.MakeStorageLoadPackagePath(getStgResp.Directory, msg.UserID, msg.PackageID)
	if err = os.MkdirAll(outputDirPath, 0755); err != nil {
		logger.WithField("StorageID", msg.StorageID).
			Warnf("creating output directory: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "create output directory failed")
	}

	tsk := svc.taskManager.StartNew(mytask.NewStorageLoadPackage(msg.UserID, msg.PackageID, outputDirPath))
	return mq.ReplyOK(agtmq.NewStartStorageLoadPackageResp(tsk.ID()))
}

func (svc *Service) WaitStorageLoadPackage(msg *agtmq.WaitStorageLoadPackage) (*agtmq.WaitStorageLoadPackageResp, *mq.CodeMessage) {
	logger.WithField("TaskID", msg.TaskID).Debugf("wait loading package")

	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return nil, mq.Failed(errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()

		errMsg := ""
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error()
		}

		loadTsk := tsk.Body().(*mytask.StorageLoadPackage)

		return mq.ReplyOK(agtmq.NewWaitStorageLoadPackageResp(true, errMsg, loadTsk.FullPath))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			loadTsk := tsk.Body().(*mytask.StorageLoadPackage)

			return mq.ReplyOK(agtmq.NewWaitStorageLoadPackageResp(true, errMsg, loadTsk.FullPath))
		}

		return mq.ReplyOK(agtmq.NewWaitStorageLoadPackageResp(false, "", ""))
	}
}

func (svc *Service) StorageCheck(msg *agtmq.StorageCheck) (*agtmq.StorageCheckResp, *mq.CodeMessage) {
	infos, err := os.ReadDir(msg.Directory)
	if err != nil {
		logger.Warnf("list storage directory failed, err: %s", err.Error())
		return mq.ReplyOK(agtmq.NewStorageCheckResp(
			err.Error(),
			nil,
		))
	}

	dirInfos := lo.Filter(infos, func(info fs.DirEntry, index int) bool { return info.IsDir() })

	if msg.IsComplete {
		return svc.checkStorageComplete(msg, dirInfos)
	} else {
		return svc.checkStorageIncrement(msg, dirInfos)
	}
}

func (svc *Service) checkStorageIncrement(msg *agtmq.StorageCheck, dirInfos []fs.DirEntry) (*agtmq.StorageCheckResp, *mq.CodeMessage) {
	infosMap := make(map[string]fs.DirEntry)
	for _, info := range dirInfos {
		infosMap[info.Name()] = info
	}

	var entries []agtmq.StorageCheckRespEntry
	for _, obj := range msg.Packages {
		dirName := utils.MakeStorageLoadPackagePath(msg.Directory, obj.UserID, obj.PackageID)
		_, ok := infosMap[dirName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, dirName)

		} else {
			// 只要文件不存在，就删除StoragePackage表中的记录
			entries = append(entries, agtmq.NewStorageCheckRespEntry(obj.PackageID, obj.UserID, agtmq.CHECK_STORAGE_RESP_OP_DELETE))
		}
	}

	// 增量情况下，不需要对infosMap中没检查的记录进行处理

	return mq.ReplyOK(agtmq.NewStorageCheckResp(consts.StorageDirectoryStateOK, entries))
}

func (svc *Service) checkStorageComplete(msg *agtmq.StorageCheck, dirInfos []fs.DirEntry) (*agtmq.StorageCheckResp, *mq.CodeMessage) {

	infosMap := make(map[string]fs.DirEntry)
	for _, info := range dirInfos {
		infosMap[info.Name()] = info
	}

	var entries []agtmq.StorageCheckRespEntry
	for _, obj := range msg.Packages {
		dirName := utils.MakeStorageLoadPackagePath(msg.Directory, obj.UserID, obj.PackageID)
		_, ok := infosMap[dirName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, dirName)

		} else {
			// 只要文件不存在，就删除StoragePackage表中的记录
			entries = append(entries, agtmq.NewStorageCheckRespEntry(obj.PackageID, obj.UserID, agtmq.CHECK_STORAGE_RESP_OP_DELETE))
		}
	}

	return mq.ReplyOK(agtmq.NewStorageCheckResp(consts.StorageDirectoryStateOK, entries))
}

func (svc *Service) StartStorageCreatePackage(msg *agtmq.StartStorageCreatePackage) (*agtmq.StartStorageCreatePackageResp, *mq.CodeMessage) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		logger.Warnf("new coordinator client: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "new coordinator client failed")
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getStgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(msg.UserID, msg.StorageID))
	if err != nil {
		logger.WithField("StorageID", msg.StorageID).
			Warnf("getting storage info: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get storage info failed")
	}

	fullPath := filepath.Clean(filepath.Join(getStgResp.Directory, msg.Path))

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

	objIter := iterator.NewUploadingObjectIterator(fullPath, uploadFilePathes)

	if msg.Redundancy.IsRepInfo() {
		repInfo, err := msg.Redundancy.ToRepInfo()
		if err != nil {
			logger.Warnf("getting rep redundancy info: %s", err.Error())

			return nil, mq.Failed(errorcode.OperationFailed, "get rep redundancy info failed")
		}

		tsk := svc.taskManager.StartNew(mytask.NewCreateRepPackage(msg.UserID, msg.BucketID, msg.Name, objIter, repInfo, msg.NodeAffinity))
		return mq.ReplyOK(agtmq.NewStartStorageCreatePackageResp(tsk.ID()))
	}

	ecInfo, err := msg.Redundancy.ToECInfo()
	if err != nil {
		logger.Warnf("getting ec redundancy info: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get ec redundancy info failed")
	}

	tsk := svc.taskManager.StartNew(mytask.NewCreateECPackage(msg.UserID, msg.BucketID, msg.Name, objIter, ecInfo, msg.NodeAffinity))
	return mq.ReplyOK(agtmq.NewStartStorageCreatePackageResp(tsk.ID()))
}

func (svc *Service) WaitStorageCreatePackage(msg *agtmq.WaitStorageCreatePackage) (*agtmq.WaitStorageCreatePackageResp, *mq.CodeMessage) {
	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return nil, mq.Failed(errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()
	} else if !tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(false, "", 0))
	}

	if tsk.Error() != nil {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, tsk.Error().Error(), 0))
	}

	// TODO 避免判断类型
	if repTask, ok := tsk.Body().(*mytask.CreateRepPackage); ok {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, "", repTask.Result.PackageID))
	}

	if ecTask, ok := tsk.Body().(*mytask.CreateECPackage); ok {
		return mq.ReplyOK(agtmq.NewWaitStorageCreatePackageResp(true, "", ecTask.Result.PackageID))
	}

	return nil, mq.Failed(errorcode.TaskNotFound, "task not found")
}
