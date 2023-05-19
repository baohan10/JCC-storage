package task

import (
	"database/sql"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/db/model"
	mysql "gitlink.org.cn/cloudream/db/sql"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	agttsk "gitlink.org.cn/cloudream/rabbitmq/message/agent/task"
	"gitlink.org.cn/cloudream/scanner/internal/config"
	"gitlink.org.cn/cloudream/utils/logger"
)

type AgentCheckStateTask struct {
	NodeIDs []int
}

func NewAgentCheckStateTask(nodeIDs []int) AgentCheckStateTask {
	return AgentCheckStateTask{
		NodeIDs: nodeIDs,
	}
}

func (t *AgentCheckStateTask) TryMerge(other Task) bool {
	task, ok := other.(*AgentCheckStateTask)
	if !ok {
		return false
	}

	t.NodeIDs = lo.Union(t.NodeIDs, task.NodeIDs)
	return true
}

func (t *AgentCheckStateTask) Execute(execCtx *ExecuteContext, execOpts ExecuteOption) {
	for _, nodeID := range t.NodeIDs {
		node, err := mysql.Node.GetByID(execCtx.DB.SQLCtx(), nodeID)
		if err == sql.ErrNoRows {
			continue
		}

		if err != nil {
			logger.WithField("NodeID", nodeID).Warnf("get node by id failed, err: %s", err.Error())
			continue
		}

		if node.State != consts.NODE_STATE_NORMAL {
			continue
		}

		// 检查上次上报时间，超时的设置为不可用
		if time.Since(node.LastReportTime) > time.Duration(config.Cfg().NodeUnavailableSeconds)*time.Second {
			err := mysql.Node.ChangeState(execCtx.DB.SQLCtx(), nodeID, consts.NODE_STATE_UNAVAILABLE)
			if err != nil {
				logger.WithField("NodeID", nodeID).Warnf("set node state failed, err: %s", err.Error())
				continue
			}

			caches, err := mysql.Cache.GetNodeCaches(execCtx.DB.SQLCtx(), nodeID)
			if err != nil {
				logger.WithField("NodeID", nodeID).Warnf("get node caches failed, err: %s", err.Error())
				continue
			}

			// 补充备份数
			execCtx.Executor.Post(NewCheckRepCountTask(lo.Map(caches, func(ch model.Cache, index int) string { return ch.HashValue })))
			continue
		}

		agentClient, err := agtcli.NewAgentClient(nodeID, &config.Cfg().RabbitMQ)
		if err != nil {
			logger.WithField("NodeID", nodeID).Warnf("create agent client failed, err: %s", err.Error())
			continue
		}
		defer agentClient.Close()

		// 紧急任务
		err = agentClient.PostTask(agtmsg.NewPostTaskBody(agttsk.NewCheckStateTask(), true, true))
		if err != nil {
			logger.WithField("NodeID", nodeID).Warnf("request to agent failed, err: %s", err.Error())
		}
	}
}
