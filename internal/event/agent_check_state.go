package event

import (
	"database/sql"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db/model"
	mysql "gitlink.org.cn/cloudream/db/sql"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtevt "gitlink.org.cn/cloudream/rabbitmq/message/agent/event"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
	"gitlink.org.cn/cloudream/scanner/internal/config"
)

type AgentCheckState struct {
	NodeID int
}

func NewAgentCheckState(nodeID int) *AgentCheckState {
	return &AgentCheckState{
		NodeID: nodeID,
	}
}

func (t *AgentCheckState) TryMerge(other Event) bool {
	event, ok := other.(*AgentCheckState)
	if !ok {
		return false
	}

	return t.NodeID == event.NodeID
}

func (t *AgentCheckState) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckState]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t))

	node, err := mysql.Node.GetByID(execCtx.Args.DB.SQLCtx(), t.NodeID)
	if err == sql.ErrNoRows {
		return
	}

	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("get node by id failed, err: %s", err.Error())
		return
	}

	if node.State != consts.NODE_STATE_NORMAL {
		return
	}

	// 检查上次上报时间，超时的设置为不可用
	// TODO 没有上报过是否要特殊处理？
	if node.LastReportTime == nil && time.Since(*node.LastReportTime) > time.Duration(config.Cfg().NodeUnavailableSeconds)*time.Second {
		err := mysql.Node.ChangeState(execCtx.Args.DB.SQLCtx(), t.NodeID, consts.NODE_STATE_UNAVAILABLE)
		if err != nil {
			log.WithField("NodeID", t.NodeID).Warnf("set node state failed, err: %s", err.Error())
			return
		}

		caches, err := execCtx.Args.DB.Cache().GetNodeCaches(execCtx.Args.DB.SQLCtx(), t.NodeID)
		if err != nil {
			log.WithField("NodeID", t.NodeID).Warnf("get node caches failed, err: %s", err.Error())
			return
		}

		// 补充备份数
		execCtx.Executor.Post(NewCheckRepCount(lo.Map(caches, func(ch model.Cache, index int) string { return ch.FileHash })))
		return
	}

	agentClient, err := agtcli.NewAgentClient(t.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer agentClient.Close()

	// 紧急任务
	err = agentClient.PostEvent(agtevt.NewCheckState(), true, true)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("request to agent failed, err: %s", err.Error())
	}
}

func init() {
	RegisterMessageConvertor(func(msg scevt.AgentCheckState) Event { return NewAgentCheckState(msg.NodeID) })
}
