package event

import (
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type AgentCheckCache struct {
	*scevt.AgentCheckCache
}

func NewAgentCheckCache(evt *scevt.AgentCheckCache) *AgentCheckCache {
	return &AgentCheckCache{
		AgentCheckCache: evt,
	}
}

func (t *AgentCheckCache) TryMerge(other Event) bool {
	event, ok := other.(*AgentCheckCache)
	if !ok {
		return false
	}

	if event.NodeID != t.NodeID {
		return false
	}

	return true
}

func (t *AgentCheckCache) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckCache]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentCheckCache))
	defer log.Debugf("end")

	// TODO unavailable的节点需不需要发送任务？

	agtCli, err := stgglb.AgentMQPool.Acquire(t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	checkResp, err := agtCli.CheckCache(agtmq.NewCheckCache(), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("checking ipfs: %s", err.Error())
		return
	}

	realFileHashes := lo.SliceToMap(checkResp.FileHashes, func(hash string) (string, bool) { return hash, true })

	// 根据IPFS中实际文件情况修改元数据。修改过程中的失败均忽略。（但关联修改需要原子性）
	execCtx.Args.DB.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		t.checkCache(execCtx, tx, realFileHashes)

		t.checkPinnedObject(execCtx, tx, realFileHashes)

		t.checkObjectBlock(execCtx, tx, realFileHashes)
		return nil
	})
}

// 对比Cache表中的记录，多了增加，少了删除
func (t *AgentCheckCache) checkCache(execCtx ExecuteContext, tx *sqlx.Tx, realFileHashes map[string]bool) {
	log := logger.WithType[AgentCheckCache]("Event")

	caches, err := execCtx.Args.DB.Cache().GetByNodeID(tx, t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("getting caches by node id: %s", err.Error())
		return
	}

	realFileHashesCp := make(map[string]bool)
	for k, v := range realFileHashes {
		realFileHashesCp[k] = v
	}

	var rms []string
	for _, c := range caches {
		if realFileHashesCp[c.FileHash] {
			// Cache表使用FileHash和NodeID作为主键，
			// 所以通过同一个NodeID查询的结果不会存在两条相同FileHash的情况
			delete(realFileHashesCp, c.FileHash)
			continue
		}
		rms = append(rms, c.FileHash)
	}

	if len(rms) > 0 {
		err = execCtx.Args.DB.Cache().NodeBatchDelete(tx, t.NodeID, rms)
		if err != nil {
			log.Warnf("batch delete node caches: %w", err.Error())
		}
	}

	if len(realFileHashesCp) > 0 {
		err = execCtx.Args.DB.Cache().BatchCreate(tx, lo.Keys(realFileHashesCp), t.NodeID, 0)
		if err != nil {
			log.Warnf("batch create node caches: %w", err)
			return
		}
	}
}

// 对比PinnedObject表，多了不变，少了删除
func (t *AgentCheckCache) checkPinnedObject(execCtx ExecuteContext, tx *sqlx.Tx, realFileHashes map[string]bool) {
	log := logger.WithType[AgentCheckCache]("Event")

	objs, err := execCtx.Args.DB.PinnedObject().GetObjectsByNodeID(tx, t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("getting pinned objects by node id: %s", err.Error())
		return
	}

	var rms []cdssdk.ObjectID
	for _, c := range objs {
		if realFileHashes[c.FileHash] {
			continue
		}
		rms = append(rms, c.ObjectID)
	}

	if len(rms) > 0 {
		err = execCtx.Args.DB.PinnedObject().NodeBatchDelete(tx, t.NodeID, rms)
		if err != nil {
			log.Warnf("batch delete node pinned objects: %s", err.Error())
		}
	}
}

// 对比ObjectBlock表，多了不变，少了删除
func (t *AgentCheckCache) checkObjectBlock(execCtx ExecuteContext, tx *sqlx.Tx, realFileHashes map[string]bool) {
	log := logger.WithType[AgentCheckCache]("Event")

	blocks, err := execCtx.Args.DB.ObjectBlock().GetByNodeID(tx, t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("getting object blocks by node id: %s", err.Error())
		return
	}

	var rms []string
	for _, b := range blocks {
		if realFileHashes[b.FileHash] {
			continue
		}
		rms = append(rms, b.FileHash)
	}

	if len(rms) > 0 {
		err = execCtx.Args.DB.ObjectBlock().NodeBatchDelete(tx, t.NodeID, rms)
		if err != nil {
			log.Warnf("batch delete node object blocks: %s", err.Error())
		}
	}
}

func init() {
	RegisterMessageConvertor(NewAgentCheckCache)
}
