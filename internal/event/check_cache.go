package event

import (
	"time"

	shell "github.com/ipfs/go-ipfs-api"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/common/consts"
	evcst "gitlink.org.cn/cloudream/common/consts/event"
	"gitlink.org.cn/cloudream/common/utils/logger"
	"gitlink.org.cn/cloudream/db/model"
	agtevt "gitlink.org.cn/cloudream/rabbitmq/message/agent/event"
	scmsg "gitlink.org.cn/cloudream/rabbitmq/message/scanner"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

type CheckCache struct {
	agtevt.CheckCache
}

func NewCheckCache(isComplete bool, caches []model.Cache) *CheckCache {
	return &CheckCache{
		CheckCache: agtevt.NewCheckCache(isComplete, caches),
	}
}

func (t *CheckCache) TryMerge(other Event) bool {
	event, ok := other.(*CheckCache)
	if !ok {
		return false
	}

	if event.IsComplete {
		t.IsComplete = true
		t.Caches = event.Caches
		return true
	}

	if !t.IsComplete {
		t.Caches = append(t.Caches, event.Caches...)
		t.Caches = lo.UniqBy(t.Caches, func(ch model.Cache) string { return ch.FileHash })
		return true
	}

	return false
}

func (t *CheckCache) Execute(execCtx ExecuteContext) {
	logger.Debugf("begin check cache")

	filesMap, err := execCtx.Args.IPFS.GetPinnedFiles()
	if err != nil {
		logger.Warnf("get pinned files from ipfs failed, err: %s", err.Error())
		return
	}

	// TODO 根据锁定清单过滤被锁定的文件的记录
	if t.IsComplete {
		t.checkComplete(filesMap, execCtx)
	} else {
		t.checkIncrement(filesMap, execCtx)
	}
}

func (t *CheckCache) checkIncrement(filesMap map[string]shell.PinInfo, execCtx ExecuteContext) {
	var updateCacheOps []scevt.UpdateCacheEntry

	for _, cache := range t.Caches {
		_, ok := filesMap[cache.FileHash]
		if ok {
			if cache.State == consts.CACHE_STATE_PINNED {
				// 不处理
			} else if cache.State == consts.CACHE_STATE_TEMP {
				err := execCtx.Args.IPFS.Unpin(cache.FileHash)
				if err != nil {
					logger.WithField("FileHash", cache.FileHash).Warnf("unpin file failed, err: %s", err.Error())
				}
			}

			// 删除map中的记录，表示此记录已被检查过
			delete(filesMap, cache.FileHash)

		} else {
			if cache.State == consts.CACHE_STATE_PINNED {
				// TODO 需要考虑此处是否是同步的过程
				err := execCtx.Args.IPFS.Pin(cache.FileHash)
				if err != nil {
					logger.WithField("FileHash", cache.FileHash).Warnf("pin file failed, err: %s", err.Error())
				}

			} else if cache.State == consts.CACHE_STATE_TEMP {
				if time.Since(cache.CacheTime) > time.Duration(config.Cfg().TempFileLifetime)*time.Second {
					updateCacheOps = append(updateCacheOps, scevt.NewUpdateCacheEntry(cache.FileHash, evcst.UPDATE_CACHE_DELETE_TEMP))
				}
			}
		}
	}

	// 增量情况下，不需要对filesMap中没检查的记录进行处理

	if len(updateCacheOps) > 0 {
		evtmsg, err := scmsg.NewPostEventBody(
			scevt.NewUpdateCache(config.Cfg().ID, updateCacheOps),
			execCtx.Option.IsEmergency,
			execCtx.Option.DontMerge,
		)
		if err == nil {
			execCtx.Args.Scanner.PostEvent(evtmsg)
		} else {
			logger.Warnf("new post event body failed, err: %s", err.Error())
		}
	}
}

func (t *CheckCache) checkComplete(filesMap map[string]shell.PinInfo, execCtx ExecuteContext) {
	var updateCacheOps []scevt.UpdateCacheEntry

	for _, cache := range t.Caches {
		_, ok := filesMap[cache.FileHash]
		if ok {
			if cache.State == consts.CACHE_STATE_PINNED {
				// 不处理
			} else if cache.State == consts.CACHE_STATE_TEMP {
				err := execCtx.Args.IPFS.Unpin(cache.FileHash)
				if err != nil {
					logger.WithField("FileHash", cache.FileHash).Warnf("unpin file failed, err: %s", err.Error())
				}
			}

			// 删除map中的记录，表示此记录已被检查过
			delete(filesMap, cache.FileHash)

		} else {
			if cache.State == consts.CACHE_STATE_PINNED {
				// TODO 需要考虑此处是否是同步的过程
				err := execCtx.Args.IPFS.Pin(cache.FileHash)
				if err != nil {
					logger.WithField("FileHash", cache.FileHash).Warnf("pin file failed, err: %s", err.Error())
				}

			} else if cache.State == consts.CACHE_STATE_TEMP {
				if time.Since(cache.CacheTime) > time.Duration(config.Cfg().TempFileLifetime)*time.Second {
					updateCacheOps = append(updateCacheOps, scevt.NewUpdateCacheEntry(cache.FileHash, evcst.UPDATE_CACHE_DELETE_TEMP))
				}
			}
		}
	}

	// map中剩下的数据是没有被遍历过，即Cache中没有记录的
	for hash, _ := range filesMap {
		updateCacheOps = append(updateCacheOps, scevt.NewUpdateCacheEntry(hash, evcst.UPDATE_CACHE_CREATE_TEMP))
	}

	evtmsg, err := scmsg.NewPostEventBody(
		scevt.NewUpdateCache(config.Cfg().ID, updateCacheOps),
		execCtx.Option.IsEmergency,
		execCtx.Option.DontMerge,
	)
	if err == nil {
		execCtx.Args.Scanner.PostEvent(evtmsg)
	} else {
		logger.Warnf("new post event body failed, err: %s", err.Error())
	}
}

func init() {
	Register(func(val agtevt.CheckCache) Event { return NewCheckCache(val.IsComplete, val.Caches) })
}
