package event

import (
	"database/sql"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/db/model"
	"gitlink.org.cn/cloudream/rabbitmq"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
	"gitlink.org.cn/cloudream/storage-scanner/internal/config"
)

type AgentCheckStorage struct {
	scevt.AgentCheckStorage
}

func NewAgentCheckStorage(storageID int64, objectIDs []int64) *AgentCheckStorage {
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
	defer log.Debugf("end")

	// 读取数据的地方就不加锁了，因为check任务会反复执行，单次失败问题不大

	stg, err := execCtx.Args.DB.Storage().GetByID(execCtx.Args.DB.SQLCtx(), t.StorageID)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithField("StorageID", t.StorageID).Warnf("get storage failed, err: %s", err.Error())
		}
		return
	}

	node, err := execCtx.Args.DB.Node().GetByID(execCtx.Args.DB.SQLCtx(), stg.NodeID)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithField("StorageID", t.StorageID).Warnf("get storage node failed, err: %s", err.Error())
		}
		return
	}

	// TODO unavailable的节点需不需要发送任务？
	if node.State != consts.NodeStateNormal {
		return
	}

	if t.ObjectIDs == nil {
		t.checkComplete(execCtx, stg)
	} else {
		t.checkIncrement(execCtx, stg)
	}
}

func (t *AgentCheckStorage) checkComplete(execCtx ExecuteContext, stg model.Storage) {
	log := logger.WithType[AgentCheckStorage]("Event")

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 全量模式下查询、修改Move记录
		StorageObject().WriteAny().
		Storage().
		// 全量模式下删除对象文件
		WriteAnyObject(t.StorageID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	objects, err := execCtx.Args.DB.StorageObject().GetAllByStorageID(execCtx.Args.DB.SQLCtx(), t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("get storage objects failed, err: %s", err.Error())
		return
	}

	t.startCheck(execCtx, stg, true, objects)
}

func (t *AgentCheckStorage) checkIncrement(execCtx ExecuteContext, stg model.Storage) {
	log := logger.WithType[AgentCheckStorage]("Event")

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 全量模式下查询、修改Move记录。因为可能有多个User Move相同的文件，所以只能用集合Write锁
		StorageObject().WriteAny().
		Storage().
		// 全量模式下删除对象文件。因为可能有多个User Move相同的文件，所以只能用集合Write锁
		WriteAnyObject(t.StorageID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	var objects []model.StorageObject
	for _, objID := range t.ObjectIDs {
		objs, err := execCtx.Args.DB.StorageObject().GetAllByStorageAndObjectID(execCtx.Args.DB.SQLCtx(), t.StorageID, objID)
		if err != nil {
			log.WithField("StorageID", t.StorageID).
				WithField("ObjectID", objID).
				Warnf("get storage object failed, err: %s", err.Error())
			return
		}

		objects = append(objects, objs...)
	}

	t.startCheck(execCtx, stg, false, objects)
}

func (t *AgentCheckStorage) startCheck(execCtx ExecuteContext, stg model.Storage, isComplete bool, objects []model.StorageObject) {
	log := logger.WithType[AgentCheckStorage]("Event")

	// 投递任务
	agentClient, err := agtcli.NewClient(stg.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer agentClient.Close()

	checkResp, err := agentClient.StorageCheck(agtmsg.NewStorageCheck(stg.StorageID, stg.Directory, isComplete, objects), rabbitmq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("checking storage: %s", err.Error())
		return
	}

	// 根据返回结果修改数据库
	var chkObjIDs []int64
	for _, entry := range checkResp.Entries {
		switch entry.Operation {
		case agtmsg.CHECK_STORAGE_RESP_OP_DELETE:
			err := execCtx.Args.DB.StorageObject().Delete(execCtx.Args.DB.SQLCtx(), t.StorageID, entry.ObjectID, entry.UserID)
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
			err := execCtx.Args.DB.StorageObject().SetStateNormal(execCtx.Args.DB.SQLCtx(), t.StorageID, entry.ObjectID, entry.UserID)
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
