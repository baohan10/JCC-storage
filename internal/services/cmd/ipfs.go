package cmd

import (
	"time"

	shell "github.com/ipfs/go-ipfs-api"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-agent/internal/config"
	"gitlink.org.cn/cloudream/storage-agent/internal/task"
	"gitlink.org.cn/cloudream/storage-common/consts"
	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
)

func (svc *Service) CheckIPFS(msg *agtmq.CheckIPFS) (*agtmq.CheckIPFSResp, *mq.CodeMessage) {
	filesMap, err := svc.ipfs.GetPinnedFiles()
	if err != nil {
		logger.Warnf("get pinned files from ipfs failed, err: %s", err.Error())
		return mq.ReplyFailed[agtmq.CheckIPFSResp](errorcode.OperationFailed, "get pinned files from ipfs failed")
	}

	// TODO 根据锁定清单过滤被锁定的文件的记录
	if msg.IsComplete {
		return svc.checkComplete(msg, filesMap)
	} else {
		return svc.checkIncrement(msg, filesMap)
	}
}

func (svc *Service) checkIncrement(msg *agtmq.CheckIPFS, filesMap map[string]shell.PinInfo) (*agtmq.CheckIPFSResp, *mq.CodeMessage) {
	var entries []agtmq.CheckIPFSRespEntry
	for _, cache := range msg.Caches {
		_, ok := filesMap[cache.FileHash]
		if ok {
			if cache.State == consts.CacheStatePinned {
				// 不处理
			} else if cache.State == consts.CacheStateTemp {
				logger.WithField("FileHash", cache.FileHash).Debugf("unpin for cache entry state is temp")
				err := svc.ipfs.Unpin(cache.FileHash)
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
					entries = append(entries, agtmq.NewCheckIPFSRespEntry(cache.FileHash, agtmq.CHECK_IPFS_RESP_OP_DELETE_TEMP))
				}
			}
		}
	}

	// 增量情况下，不需要对filesMap中没检查的记录进行处理

	return mq.ReplyOK(agtmq.NewCheckIPFSResp(entries))
}

func (svc *Service) checkComplete(msg *agtmq.CheckIPFS, filesMap map[string]shell.PinInfo) (*agtmq.CheckIPFSResp, *mq.CodeMessage) {
	var entries []agtmq.CheckIPFSRespEntry
	for _, cache := range msg.Caches {
		_, ok := filesMap[cache.FileHash]
		if ok {
			if cache.State == consts.CacheStatePinned {
				// 不处理
			} else if cache.State == consts.CacheStateTemp {
				logger.WithField("FileHash", cache.FileHash).Debugf("unpin for cache entry state is temp")
				err := svc.ipfs.Unpin(cache.FileHash)
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
					entries = append(entries, agtmq.NewCheckIPFSRespEntry(cache.FileHash, agtmq.CHECK_IPFS_RESP_OP_DELETE_TEMP))
				}
			}
		}
	}

	// map中剩下的数据是没有被遍历过，即Cache中没有记录的，那么就Unpin文件，并产生一条Temp记录
	for hash := range filesMap {
		logger.WithField("FileHash", hash).Debugf("unpin for no cacah entry")
		err := svc.ipfs.Unpin(hash)
		if err != nil {
			logger.WithField("FileHash", hash).Warnf("unpin file failed, err: %s", err.Error())
		}
		entries = append(entries, agtmq.NewCheckIPFSRespEntry(hash, agtmq.CHECK_IPFS_RESP_OP_CREATE_TEMP))
	}

	return mq.ReplyOK(agtmq.NewCheckIPFSResp(entries))
}
