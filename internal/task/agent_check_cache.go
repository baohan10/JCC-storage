package task

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/db/model"
	mysql "gitlink.org.cn/cloudream/db/sql"
	"gitlink.org.cn/cloudream/scanner/internal/config"
	log "gitlink.org.cn/cloudream/utils/logger"

	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	agttsk "gitlink.org.cn/cloudream/rabbitmq/message/agent/task"
)

type AgentCheckCacheTaskEntry struct {
	NodeID     int
	FileHashes []string // 需要检查的FileHash列表，如果为nil（不是为空），则代表进行全量检查
}

func NewAgentCheckCacheTaskEntry(nodeID int, fileHashes []string) AgentCheckCacheTaskEntry {
	return AgentCheckCacheTaskEntry{
		NodeID:     nodeID,
		FileHashes: fileHashes,
	}
}

type AgentCheckCacheTask struct {
	Entries []AgentCheckCacheTaskEntry
}

func NewAgentCheckCacheTask(entries []AgentCheckCacheTaskEntry) *AgentCheckCacheTask {
	return &AgentCheckCacheTask{
		Entries: entries,
	}
}

func (t *AgentCheckCacheTask) TryMerge(other Task) bool {
	chkTask, ok := other.(*AgentCheckCacheTask)
	if !ok {
		return false
	}

	for _, entry := range chkTask.Entries {
		_, index, ok := lo.FindIndexOf(t.Entries, func(e AgentCheckCacheTaskEntry) bool { return e.NodeID == entry.NodeID })
		if ok {
			myEntry := &t.Entries[index]

			// FileHashes为nil时代表全量检查
			if entry.FileHashes == nil {
				myEntry.FileHashes = nil
			} else if myEntry.FileHashes != nil {
				myEntry.FileHashes = lo.Union(myEntry.FileHashes, entry.FileHashes)
			}

		} else {
			t.Entries = append(t.Entries, entry)
		}
	}

	return true
}

func (t *AgentCheckCacheTask) Execute(execCtx *ExecuteContext, execOpts ExecuteOption) {
	for _, entry := range t.Entries {
		err := t.checkOneAgentCache(entry, execCtx, execOpts)
		if err != nil {
			log.Warnf("let agent check cache failed, err: %s", err.Error())
			continue
		}
	}
}

func (t *AgentCheckCacheTask) checkOneAgentCache(entry AgentCheckCacheTaskEntry, execCtx *ExecuteContext, execOpts ExecuteOption) error {
	var isComplete bool
	var caches []model.Cache

	err := execCtx.DB.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		// TODO unavailable的节点需不需要发送任务？

		if entry.FileHashes == nil {
			var err error
			caches, err = mysql.Cache.GetNodeCaches(tx, entry.NodeID)
			if err != nil {
				return fmt.Errorf("get node caches failed, err: %w", err)
			}
			isComplete = true

		} else {
			for _, hash := range entry.FileHashes {
				ch, err := mysql.Cache.Get(tx, hash, entry.NodeID)
				// 记录不存在则跳过
				if err == sql.ErrNoRows {
					continue
				}

				if err != nil {
					return fmt.Errorf("get cache failed, err: %w", err)
				}

				caches = append(caches, ch)
			}
			isComplete = false
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := agtcli.NewAgentClient(entry.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", entry.NodeID, err)
	}
	defer agentClient.Close()

	err = agentClient.PostTask(agtmsg.NewPostTaskBody(
		agttsk.NewCheckCacheTask(isComplete, caches),
		execOpts.IsEmergency, // 继承本任务的执行选项
		execOpts.DontMerge))
	if err != nil {
		return fmt.Errorf("request to agent %d failed, err: %w", entry.NodeID, err)
	}

	return nil
}
