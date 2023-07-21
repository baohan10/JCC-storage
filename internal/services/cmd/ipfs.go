package cmd

import (
	"time"

	shell "github.com/ipfs/go-ipfs-api"
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/agent/internal/task"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
)

func (svc *Service) CheckIPFS(msg *agtmsg.CheckIPFS) (*agtmsg.CheckIPFSResp, *ramsg.CodeMessage) {
	filesMap, err := svc.ipfs.GetPinnedFiles()
	if err != nil {
		logger.Warnf("get pinned files from ipfs failed, err: %s", err.Error())
		return ramsg.ReplyFailed[agtmsg.CheckIPFSResp](errorcode.OPERATION_FAILED, "get pinned files from ipfs failed")
	}

	// TODO 根据锁定清单过滤被锁定的文件的记录
	if msg.IsComplete {
		return svc.checkComplete(msg, filesMap)
	} else {
		return svc.checkIncrement(msg, filesMap)
	}
}

func (svc *Service) checkIncrement(msg *agtmsg.CheckIPFS, filesMap map[string]shell.PinInfo) (*agtmsg.CheckIPFSResp, *ramsg.CodeMessage) {
	var entries []agtmsg.CheckIPFSRespEntry
	for _, cache := range msg.Caches {
		_, ok := filesMap[cache.FileHash]
		if ok {
			if cache.State == consts.CACHE_STATE_PINNED {
				// 不处理
			} else if cache.State == consts.CACHE_STATE_TEMP {
				err := svc.ipfs.Unpin(cache.FileHash)
				if err != nil {
					logger.WithField("FileHash", cache.FileHash).Warnf("unpin file failed, err: %s", err.Error())
				}
			}

			// 删除map中的记录，表示此记录已被检查过
			delete(filesMap, cache.FileHash)

		} else {
			if cache.State == consts.CACHE_STATE_PINNED {
				svc.taskManager.StartComparable(task.NewIPFSPin(cache.FileHash))

			} else if cache.State == consts.CACHE_STATE_TEMP {
				if time.Since(cache.CacheTime) > time.Duration(config.Cfg().TempFileLifetime)*time.Second {
					entries = append(entries, agtmsg.NewCheckIPFSRespEntry(cache.FileHash, agtmsg.CHECK_IPFS_RESP_OP_DELETE_TEMP))
				}
			}
		}
	}

	// 增量情况下，不需要对filesMap中没检查的记录进行处理

	return ramsg.ReplyOK(agtmsg.NewCheckIPFSResp(entries))
}

func (svc *Service) checkComplete(msg *agtmsg.CheckIPFS, filesMap map[string]shell.PinInfo) (*agtmsg.CheckIPFSResp, *ramsg.CodeMessage) {
	var entries []agtmsg.CheckIPFSRespEntry
	for _, cache := range msg.Caches {
		_, ok := filesMap[cache.FileHash]
		if ok {
			if cache.State == consts.CACHE_STATE_PINNED {
				// 不处理
			} else if cache.State == consts.CACHE_STATE_TEMP {
				err := svc.ipfs.Unpin(cache.FileHash)
				if err != nil {
					logger.WithField("FileHash", cache.FileHash).Warnf("unpin file failed, err: %s", err.Error())
				}
			}

			// 删除map中的记录，表示此记录已被检查过
			delete(filesMap, cache.FileHash)

		} else {
			if cache.State == consts.CACHE_STATE_PINNED {
				svc.taskManager.StartComparable(task.NewIPFSPin(cache.FileHash))

			} else if cache.State == consts.CACHE_STATE_TEMP {
				if time.Since(cache.CacheTime) > time.Duration(config.Cfg().TempFileLifetime)*time.Second {
					entries = append(entries, agtmsg.NewCheckIPFSRespEntry(cache.FileHash, agtmsg.CHECK_IPFS_RESP_OP_DELETE_TEMP))
				}
			}
		}
	}

	// map中剩下的数据是没有被遍历过，即Cache中没有记录的，那么就Unpin文件，并产生一条Temp记录
	for hash := range filesMap {
		err := svc.ipfs.Unpin(hash)
		if err != nil {
			logger.WithField("FileHash", hash).Warnf("unpin file failed, err: %s", err.Error())
		}
		entries = append(entries, agtmsg.NewCheckIPFSRespEntry(hash, agtmsg.CHECK_IPFS_RESP_OP_CREATE_TEMP))
	}

	return ramsg.ReplyOK(agtmsg.NewCheckIPFSResp(entries))
}
