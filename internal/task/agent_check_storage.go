package task

import (
	"database/sql"

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

type AgentCheckStorageTask struct {
	StorageID int
	ObjectIDs []int // 需要检查的Object文件列表，如果为nil（不是为空），则代表进行全量检查
}

func (t *AgentCheckStorageTask) TryMerge(other Task) bool {
	task, ok := other.(*AgentCheckStorageTask)
	if !ok {
		return false
	}

	if t.StorageID != task.StorageID {
		return false
	}

	// ObjectIDs为nil时代表全量检查
	if task.ObjectIDs == nil {
		t.ObjectIDs = nil
	} else if t.ObjectIDs != nil {
		t.ObjectIDs = lo.Union(t.ObjectIDs, task.ObjectIDs)
	}

	return true
}

func (t *AgentCheckStorageTask) Execute(execCtx *ExecuteContext, execOpts ExecuteOption) {
	stg, err := mysql.Storage.GetByID(execCtx.DB.SQLCtx(), t.StorageID)
	if err != nil {
		if err != sql.ErrNoRows {
			logger.WithField("StorageID", t.StorageID).Warnf("get storage failed, err: %s", err.Error())
		}
		return
	}

	node, err := mysql.Node.GetByID(execCtx.DB.SQLCtx(), stg.NodeID)
	if err != nil {
		if err != sql.ErrNoRows {
			logger.WithField("StorageID", t.StorageID).Warnf("get storage node failed, err: %s", err.Error())
		}
		return
	}

	// TODO unavailable的节点需不需要发送任务？
	if node.State != consts.NODE_STATE_NORMAL {
		return
	}

	// 获取对象信息
	var isComplete bool
	var objects []model.StorageObject
	if t.ObjectIDs == nil {
		var err error
		objects, err = mysql.StorageObject.GetAllByStorageID(execCtx.DB.SQLCtx(), t.StorageID)
		if err != nil {
			logger.WithField("StorageID", t.StorageID).Warnf("get storage objects failed, err: %s", err.Error())
			return
		}
		isComplete = true
	} else {
		for _, objID := range t.ObjectIDs {
			obj, err := mysql.StorageObject.Get(execCtx.DB.SQLCtx(), t.StorageID, objID)
			if err == sql.ErrNoRows {
				continue
			}
			if err != nil {
				logger.WithField("StorageID", t.StorageID).
					WithField("ObjectID", objID).
					Warnf("get storage object failed, err: %s", err.Error())
				return
			}

			objects = append(objects, obj)
		}
		isComplete = false
	}

	// 投递任务
	agentClient, err := agtcli.NewAgentClient(stg.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		logger.WithField("NodeID", stg.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer agentClient.Close()

	err = agentClient.PostTask(agtmsg.NewPostTaskBody(
		agttsk.NewCheckStorageTask(isComplete, objects),
		execOpts.IsEmergency, // 继承本任务的执行选项
		execOpts.DontMerge))
	if err != nil {
		logger.WithField("NodeID", stg.NodeID).Warnf("request to agent failed, err: %s", stg.NodeID, err.Error())
	}
}
