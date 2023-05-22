package event

import (
	"database/sql"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/utils/logger"
	"gitlink.org.cn/cloudream/db/model"
	mysql "gitlink.org.cn/cloudream/db/sql"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	agttsk "gitlink.org.cn/cloudream/rabbitmq/message/agent/event"
	"gitlink.org.cn/cloudream/scanner/internal/config"
)

type AgentCheckState struct {
	NodeIDs []int
}

func NewAgentCheckState(nodeIDs []int) AgentCheckState {
	return AgentCheckState{
		NodeIDs: nodeIDs,
	}
}

func (t *AgentCheckState) TryMerge(other Event) bool {
	event, ok := other.(*AgentCheckState)
	if !ok {
		return false
	}

	t.NodeIDs = lo.Union(t.NodeIDs, event.NodeIDs)
	return true
}

func (t *AgentCheckState) Execute(execCtx ExecuteContext) {
	for _, nodeID := range t.NodeIDs {
		node, err := mysql.Node.GetByID(execCtx.Args.DB.SQLCtx(), nodeID)
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
			err := mysql.Node.ChangeState(execCtx.Args.DB.SQLCtx(), nodeID, consts.NODE_STATE_UNAVAILABLE)
			if err != nil {
				logger.WithField("NodeID", nodeID).Warnf("set node state failed, err: %s", err.Error())
				continue
			}

			caches, err := mysql.Cache.GetNodeCaches(execCtx.Args.DB.SQLCtx(), nodeID)
			if err != nil {
				logger.WithField("NodeID", nodeID).Warnf("get node caches failed, err: %s", err.Error())
				continue
			}

			// 补充备份数
			execCtx.Executor.Post(NewCheckRepCount(lo.Map(caches, func(ch model.Cache, index int) string { return ch.HashValue })))
			continue
		}

		agentClient, err := agtcli.NewAgentClient(nodeID, &config.Cfg().RabbitMQ)
		if err != nil {
			logger.WithField("NodeID", nodeID).Warnf("create agent client failed, err: %s", err.Error())
			continue
		}
		defer agentClient.Close()

		// 紧急任务
		err = agentClient.PostEvent(agtmsg.NewPostEventBody(agttsk.NewCheckState(), true, true))
		if err != nil {
			logger.WithField("NodeID", nodeID).Warnf("request to agent failed, err: %s", err.Error())
		}
	}
}
