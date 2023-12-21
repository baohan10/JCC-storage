package mq

import (
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mytask "gitlink.org.cn/cloudream/storage/agent/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

func (svc *Service) CheckCache(msg *agtmq.CheckCache) (*agtmq.CheckCacheResp, *mq.CodeMessage) {
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		logger.Warnf("new ipfs client: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "new ipfs client failed")
	}
	defer ipfsCli.Close()

	files, err := ipfsCli.GetPinnedFiles()
	if err != nil {
		logger.Warnf("get pinned files from ipfs failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get pinned files from ipfs failed")
	}

	return mq.ReplyOK(agtmq.NewCheckCacheResp(lo.Keys(files)))
}

func (svc *Service) CacheGC(msg *agtmq.CacheGC) (*agtmq.CacheGCResp, *mq.CodeMessage) {
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		logger.Warnf("new ipfs client: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "new ipfs client failed")
	}
	defer ipfsCli.Close()

	files, err := ipfsCli.GetPinnedFiles()
	if err != nil {
		logger.Warnf("get pinned files from ipfs failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get pinned files from ipfs failed")
	}

	// unpin所有没有没记录到元数据的文件
	shouldPinnedFiles := lo.SliceToMap(msg.PinnedFileHashes, func(hash string) (string, bool) { return hash, true })
	for hash := range files {
		if !shouldPinnedFiles[hash] {
			ipfsCli.Unpin(hash)
			logger.WithField("FileHash", hash).Debugf("unpinned by gc")
		}
	}

	return mq.ReplyOK(agtmq.RespCacheGC())
}

func (svc *Service) StartCacheMovePackage(msg *agtmq.StartCacheMovePackage) (*agtmq.StartCacheMovePackageResp, *mq.CodeMessage) {
	tsk := svc.taskManager.StartNew(mytask.NewCacheMovePackage(msg.UserID, msg.PackageID))
	return mq.ReplyOK(agtmq.NewStartCacheMovePackageResp(tsk.ID()))
}

func (svc *Service) WaitCacheMovePackage(msg *agtmq.WaitCacheMovePackage) (*agtmq.WaitCacheMovePackageResp, *mq.CodeMessage) {
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

		return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(true, errMsg))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(true, errMsg))
		}

		return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(false, ""))
	}
}
