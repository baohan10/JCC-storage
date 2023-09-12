package mq

import (
	"time"

	shell "github.com/ipfs/go-ipfs-api"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/agent/internal/config"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	mytask "gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/consts"
	"gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

func (svc *Service) CheckCache(msg *agtmq.CheckCache) (*agtmq.CheckCacheResp, *mq.CodeMessage) {
	ipfsCli, err := globals.IPFSPool.Acquire()
	if err != nil {
		logger.Warnf("new ipfs client: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "new ipfs client failed")
	}
	defer ipfsCli.Close()

	filesMap, err := ipfsCli.GetPinnedFiles()
	if err != nil {
		logger.Warnf("get pinned files from ipfs failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get pinned files from ipfs failed")
	}

	// TODO 根据锁定清单过滤被锁定的文件的记录
	if msg.IsComplete {
		return svc.checkComplete(msg, filesMap, ipfsCli)
	} else {
		return svc.checkIncrement(msg, filesMap, ipfsCli)
	}
}

func (svc *Service) checkIncrement(msg *agtmq.CheckCache, filesMap map[string]shell.PinInfo, ipfsCli *ipfs.PoolClient) (*agtmq.CheckCacheResp, *mq.CodeMessage) {
	var entries []agtmq.CheckIPFSRespEntry
	for _, cache := range msg.Caches {
		_, ok := filesMap[cache.FileHash]
		if ok {
			if cache.State == consts.CacheStatePinned {
				// 不处理
			} else if cache.State == consts.CacheStateTemp {
				logger.WithField("FileHash", cache.FileHash).Debugf("unpin for cache entry state is temp")
				err := ipfsCli.Unpin(cache.FileHash)
				if err != nil {
					logger.WithField("FileHash", cache.FileHash).Warnf("unpin file failed, err: %s", err.Error())
				}
			}

			// 删除map中的记录，表示此记录已被检查过
			delete(filesMap, cache.FileHash)

		} else {
			if cache.State == consts.CacheStatePinned {
				svc.taskManager.StartComparable(task.NewIPFSPin(cache.FileHash))

			} else if cache.State == consts.CacheStateTemp {
				if time.Since(cache.CacheTime) > time.Duration(config.Cfg().TempFileLifetime)*time.Second {
					entries = append(entries, agtmq.NewCheckCacheRespEntry(cache.FileHash, agtmq.CHECK_IPFS_RESP_OP_DELETE_TEMP))
				}
			}
		}
	}

	// 增量情况下，不需要对filesMap中没检查的记录进行处理

	return mq.ReplyOK(agtmq.NewCheckCacheResp(entries))
}

func (svc *Service) checkComplete(msg *agtmq.CheckCache, filesMap map[string]shell.PinInfo, ipfsCli *ipfs.PoolClient) (*agtmq.CheckCacheResp, *mq.CodeMessage) {
	var entries []agtmq.CheckIPFSRespEntry
	for _, cache := range msg.Caches {
		_, ok := filesMap[cache.FileHash]
		if ok {
			if cache.State == consts.CacheStatePinned {
				// 不处理
			} else if cache.State == consts.CacheStateTemp {
				logger.WithField("FileHash", cache.FileHash).Debugf("unpin for cache entry state is temp")
				err := ipfsCli.Unpin(cache.FileHash)
				if err != nil {
					logger.WithField("FileHash", cache.FileHash).Warnf("unpin file failed, err: %s", err.Error())
				}
			}

			// 删除map中的记录，表示此记录已被检查过
			delete(filesMap, cache.FileHash)

		} else {
			if cache.State == consts.CacheStatePinned {
				svc.taskManager.StartComparable(task.NewIPFSPin(cache.FileHash))

			} else if cache.State == consts.CacheStateTemp {
				if time.Since(cache.CacheTime) > time.Duration(config.Cfg().TempFileLifetime)*time.Second {
					entries = append(entries, agtmq.NewCheckCacheRespEntry(cache.FileHash, agtmq.CHECK_IPFS_RESP_OP_DELETE_TEMP))
				}
			}
		}
	}

	// map中剩下的数据是没有被遍历过，即Cache中没有记录的，那么就Unpin文件，并产生一条Temp记录
	for hash := range filesMap {
		logger.WithField("FileHash", hash).Debugf("unpin for no cacah entry")
		err := ipfsCli.Unpin(hash)
		if err != nil {
			logger.WithField("FileHash", hash).Warnf("unpin file failed, err: %s", err.Error())
		}
		entries = append(entries, agtmq.NewCheckCacheRespEntry(hash, agtmq.CHECK_IPFS_RESP_OP_CREATE_TEMP))
	}

	return mq.ReplyOK(agtmq.NewCheckCacheResp(entries))
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
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs)) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(true, errMsg))
		}

		return mq.ReplyOK(agtmq.NewWaitCacheMovePackageResp(false, ""))
	}
}
