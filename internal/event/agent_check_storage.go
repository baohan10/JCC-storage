package event

import (
	"database/sql"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db/model"
	mysql "gitlink.org.cn/cloudream/db/sql"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
	"gitlink.org.cn/cloudream/scanner/internal/config"
)

type AgentCheckStorage struct {
	scevt.AgentCheckStorage
}

func NewAgentCheckStorage(storageID int, objectIDs []int) *AgentCheckStorage {
	return &AgentCheckStorage{
		AgentCheckStorage: scevt.NewAgentCheckStorage(storageID, objectIDs),
	}
}

func (t *AgentCheckStorage) TryMerge(other Event) bool {
	event, ok := other.(*AgentCheckStorage)
	if !ok {
		return false
	}

	if t.StorageID != event.StorageID {
		return false
	}

	// ObjectIDs为nil时代表全量检查
	if event.ObjectIDs == nil {
		t.ObjectIDs = nil
	} else if t.ObjectIDs != nil {
		t.ObjectIDs = lo.Union(t.ObjectIDs, event.ObjectIDs)
	}

	return true
}

func (t *AgentCheckStorage) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckStorage]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t))

	stg, err := mysql.Storage.GetByID(execCtx.Args.DB.SQLCtx(), t.StorageID)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithField("StorageID", t.StorageID).Warnf("get storage failed, err: %s", err.Error())
		}
		return
	}

	node, err := mysql.Node.GetByID(execCtx.Args.DB.SQLCtx(), stg.NodeID)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithField("StorageID", t.StorageID).Warnf("get storage node failed, err: %s", err.Error())
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
		objects, err = mysql.StorageObject.GetAllByStorageID(execCtx.Args.DB.SQLCtx(), t.StorageID)
		if err != nil {
			log.WithField("StorageID", t.StorageID).Warnf("get storage objects failed, err: %s", err.Error())
			return
		}
		isComplete = true
	} else {
		for _, objID := range t.ObjectIDs {
			objs, err := mysql.StorageObject.GetAllByStorageAndObjectID(execCtx.Args.DB.SQLCtx(), t.StorageID, objID)
			if err != nil {
				log.WithField("StorageID", t.StorageID).
					WithField("ObjectID", objID).
					Warnf("get storage object failed, err: %s", err.Error())
				return
			}

			objects = append(objects, objs...)
		}
		isComplete = false
	}

	// 投递任务
	agentClient, err := agtcli.NewClient(stg.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer agentClient.Close()

	checkResp, err := agentClient.CheckStorage(agtmsg.NewCheckStorageBody(stg.StorageID, stg.Directory, isComplete, objects))
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("request to agent failed, err: %s", err.Error())
	}
	if checkResp.IsFailed() {
		log.WithField("NodeID", stg.NodeID).Warnf("agent operation failed, err: %s", err.Error())
		return
	}

	var chkObjIDs []int
	for _, entry := range checkResp.Body.Entries {
		switch entry.Operation {
		case agtmsg.CHECK_STORAGE_RESP_OP_DELETE:
			err := mysql.StorageObject.Delete(execCtx.Args.DB.SQLCtx(), t.StorageID, entry.ObjectID, entry.UserID)
			if err != nil {
				log.WithField("StorageID", t.StorageID).
					WithField("ObjectID", entry.ObjectID).
					Warnf("delete storage object failed, err: %s", err.Error())
			}
			chkObjIDs = append(chkObjIDs, entry.ObjectID)

			log.WithField("StorageID", t.StorageID).
				WithField("ObjectID", entry.ObjectID).
				WithField("UserID", entry.UserID).
				Debugf("delete storage object")

		case agtmsg.CHECK_STORAGE_RESP_OP_SET_NORMAL:
			err := mysql.StorageObject.SetStateNormal(execCtx.Args.DB.SQLCtx(), t.StorageID, entry.ObjectID, entry.UserID)
			if err != nil {
				log.WithField("StorageID", t.StorageID).
					WithField("ObjectID", entry.ObjectID).
					Warnf("change storage object state failed, err: %s", err.Error())
			}

			log.WithField("StorageID", t.StorageID).
				WithField("ObjectID", entry.ObjectID).
				WithField("UserID", entry.UserID).
				Debugf("set storage object normal")
		}
	}

	if len(chkObjIDs) > 0 {
		execCtx.Executor.Post(NewCheckObject(chkObjIDs))
	}
}

func init() {
	RegisterMessageConvertor(func(msg scevt.AgentCheckStorage) Event { return NewAgentCheckStorage(msg.StorageID, msg.ObjectIDs) })
}
