package event

import (
	"database/sql"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db/model"
	"gitlink.org.cn/cloudream/scanner/internal/config"

	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

type AgentCheckCache struct {
	scevt.AgentCheckCache
}

func NewAgentCheckCache(nodeID int, fileHashes []string) *AgentCheckCache {
	return &AgentCheckCache{
		AgentCheckCache: scevt.NewAgentCheckCache(nodeID, fileHashes),
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

	// FileHashes为nil时代表全量检查
	if event.FileHashes == nil {
		t.FileHashes = nil
	} else if t.FileHashes != nil {
		t.FileHashes = lo.Union(t.FileHashes, event.FileHashes)
	}

	return true
}

func (t *AgentCheckCache) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckCache]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t))

	// TODO unavailable的节点需不需要发送任务？

	if t.FileHashes == nil {
		t.checkComplete(execCtx)
	} else {
		t.checkIncrement(execCtx)
	}
}

func (t *AgentCheckCache) checkComplete(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckCache]("Event")

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 全量模式下修改某个节点所有的Cache记录
		Cache().WriteAny().
		IPFS().
		// 全量模式下修改某个节点所有的副本数据
		WriteAnyRep(t.NodeID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	caches, err := execCtx.Args.DB.Cache().GetNodeCaches(execCtx.Args.DB.SQLCtx(), t.NodeID)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("get node caches failed, err: %s", err.Error())
		return
	}

	t.startCheck(execCtx, true, caches)
}

func (t *AgentCheckCache) checkIncrement(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckCache]("Event")

	builder := reqbuilder.NewBuilder()
	for _, hash := range t.FileHashes {
		builder.
			// 增量模式下，不会有改动到Cache记录的操作
			Metadata().Cache().ReadOne(t.NodeID, hash).
			// 由于副本Write锁的特点，Pin文件（创建文件）不需要Create锁
			IPFS().WriteOneRep(t.NodeID, hash)
	}
	mutex, err := builder.MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	var caches []model.Cache
	for _, hash := range t.FileHashes {
		ch, err := execCtx.Args.DB.Cache().Get(execCtx.Args.DB.SQLCtx(), hash, t.NodeID)
		// 记录不存在则跳过
		if err == sql.ErrNoRows {
			continue
		}

		if err != nil {
			log.WithField("FileHash", hash).WithField("NodeID", t.NodeID).Warnf("get cache failed, err: %s", err.Error())
			return
		}

		caches = append(caches, ch)
	}

	t.startCheck(execCtx, true, caches)
}

func (t *AgentCheckCache) startCheck(execCtx ExecuteContext, isComplete bool, caches []model.Cache) {
	log := logger.WithType[AgentCheckCache]("Event")

	// 然后向代理端发送移动文件的请求
	agentClient, err := agtcli.NewClient(t.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer agentClient.Close()

	checkResp, err := agentClient.CheckIPFS(agtmsg.NewCheckIPFSBody(isComplete, caches))
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("request to agent failed, err: %s", err.Error())
		return
	}
	if checkResp.IsFailed() {
		log.WithField("NodeID", t.NodeID).Warnf("agent operation failed, err: %s", err.Error())
		return
	}

	// 根据返回结果修改数据库
	for _, entry := range checkResp.Body.Entries {
		switch entry.Operation {
		case agtmsg.CHECK_IPFS_RESP_OP_DELETE_TEMP:
			err := execCtx.Args.DB.Cache().DeleteTemp(execCtx.Args.DB.SQLCtx(), entry.FileHash, t.NodeID)
			if err != nil {
				log.WithField("FileHash", entry.FileHash).
					WithField("NodeID", t.NodeID).
					Warnf("delete temp cache failed, err: %s", err.Error())
			}

			log.WithField("FileHash", entry.FileHash).
				WithField("NodeID", t.NodeID).
				Debugf("delete temp cache")

		case agtmsg.CHECK_IPFS_RESP_OP_CREATE_TEMP:
			err := execCtx.Args.DB.Cache().CreateTemp(execCtx.Args.DB.SQLCtx(), entry.FileHash, t.NodeID)
			if err != nil {
				log.WithField("FileHash", entry.FileHash).
					WithField("NodeID", t.NodeID).
					Warnf("create temp cache failed, err: %s", err.Error())
			}

			log.WithField("FileHash", entry.FileHash).
				WithField("NodeID", t.NodeID).
				Debugf("create temp cache")
		}
	}
}

func init() {
	RegisterMessageConvertor(func(msg scevt.AgentCheckCache) Event { return NewAgentCheckCache(msg.NodeID, msg.FileHashes) })
}
