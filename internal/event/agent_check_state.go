package event

import (
	"database/sql"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db/model"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
	"gitlink.org.cn/cloudream/scanner/internal/config"
)

type AgentCheckState struct {
	scevt.AgentCheckState
}

func NewAgentCheckState(nodeID int) *AgentCheckState {
	return &AgentCheckState{
		AgentCheckState: scevt.NewAgentCheckState(nodeID),
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
	defer log.Debugf("end")

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 查询、修改节点状态
		Node().WriteOne(t.NodeID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	node, err := execCtx.Args.DB.Node().GetByID(execCtx.Args.DB.SQLCtx(), t.NodeID)
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
	if node.LastReportTime != nil && time.Since(*node.LastReportTime) > time.Duration(config.Cfg().NodeUnavailableSeconds)*time.Second {
		err := execCtx.Args.DB.Node().UpdateState(execCtx.Args.DB.SQLCtx(), t.NodeID, consts.NODE_STATE_UNAVAILABLE)
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

	agentClient, err := agtcli.NewClient(t.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer agentClient.Close()

	getResp, err := agentClient.GetState(agtmsg.NewGetStateBody())
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("request to agent failed, err: %s", err.Error())
	}
	if getResp.IsFailed() {
		log.WithField("NodeID", t.NodeID).Warnf("agent operation failed, err: %s", err.Error())
		return
	}

	// 根据返回结果修改节点状态
	if getResp.Body.IPFSState != consts.IPFS_STATE_OK {
		log.WithField("NodeID", t.NodeID).Warnf("IPFS status is %s, set node state unavailable", getResp.Body.IPFSState)

		err := execCtx.Args.DB.Node().UpdateState(execCtx.Args.DB.SQLCtx(), t.NodeID, consts.NODE_STATE_UNAVAILABLE)
		if err != nil {
			log.WithField("NodeID", t.NodeID).Warnf("change node state failed, err: %s", err.Error())
		}
		return
	}

	// TODO 如果以后还有其他的状态，要判断哪些状态下能设置Normal
	err = execCtx.Args.DB.Node().UpdateState(execCtx.Args.DB.SQLCtx(), t.NodeID, consts.NODE_STATE_NORMAL)
	if err != nil {
		log.WithField("NodeID", t.NodeID).Warnf("change node state failed, err: %s", err.Error())
	}
}

func init() {
	RegisterMessageConvertor(func(msg scevt.AgentCheckState) Event { return NewAgentCheckState(msg.NodeID) })
}
