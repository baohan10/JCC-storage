package event

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type AgentCacheGC struct {
	*scevt.AgentCacheGC
}

func NewAgentCacheGC(evt *scevt.AgentCacheGC) *AgentCacheGC {
	return &AgentCacheGC{
		AgentCacheGC: evt,
	}
}

func (t *AgentCacheGC) TryMerge(other Event) bool {
	event, ok := other.(*AgentCacheGC)
	if !ok {
		return false
	}

	if event.NodeID != t.NodeID {
		return false
	}

	return true
}

func (t *AgentCacheGC) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCacheGC]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentCacheGC))
	defer log.Debugf("end")

	// TODO unavailable的节点需不需要发送任务？

	mutex, err := reqbuilder.NewBuilder().
		// 进行GC
		IPFS().GC(t.NodeID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	var allFileHashes []string
	err = execCtx.Args.DB.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		blocks, err := execCtx.Args.DB.ObjectBlock().GetByNodeID(tx, t.NodeID)
		if err != nil {
			return fmt.Errorf("getting object blocks by node id: %w", err)
		}
		for _, c := range blocks {
			allFileHashes = append(allFileHashes, c.FileHash)
		}

		objs, err := execCtx.Args.DB.PinnedObject().GetObjectsByNodeID(tx, t.NodeID)
		if err != nil {
			return fmt.Errorf("getting pinned objects by node id: %w", err)
		}
		for _, o := range objs {
			allFileHashes = append(allFileHashes, o.FileHash)
		}

		return nil
	})
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warn(err.Error())
		return
	}

	agtCli, err := stgglb.AgentMQPool.Acquire(t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	_, err = agtCli.CacheGC(agtmq.ReqCacheGC(allFileHashes), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("ipfs gc: %s", err.Error())
		return
	}
}

func init() {
	RegisterMessageConvertor(NewAgentCacheGC)
}
