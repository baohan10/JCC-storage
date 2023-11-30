package event

import (
	"database/sql"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type AgentCheckStorage struct {
	*scevt.AgentCheckStorage
}

func NewAgentCheckStorage(storageID cdssdk.StorageID, packageIDs []cdssdk.PackageID) *AgentCheckStorage {
	return &AgentCheckStorage{
		AgentCheckStorage: scevt.NewAgentCheckStorage(storageID, packageIDs),
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

	// PackageIDs为nil时代表全量检查
	if event.PackageIDs == nil {
		t.PackageIDs = nil
	} else if t.PackageIDs != nil {
		t.PackageIDs = lo.Union(t.PackageIDs, event.PackageIDs)
	}

	return true
}

func (t *AgentCheckStorage) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentCheckStorage]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentCheckStorage))
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

	if t.PackageIDs == nil {
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
		StoragePackage().WriteAny().
		Storage().
		// 全量模式下删除对象文件
		WriteAnyPackage(t.StorageID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	packages, err := execCtx.Args.DB.StoragePackage().GetAllByStorageID(execCtx.Args.DB.SQLCtx(), t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("get storage packages failed, err: %s", err.Error())
		return
	}

	t.startCheck(execCtx, stg, true, packages)
}

func (t *AgentCheckStorage) checkIncrement(execCtx ExecuteContext, stg model.Storage) {
	log := logger.WithType[AgentCheckStorage]("Event")

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 全量模式下查询、修改Move记录。因为可能有多个User Move相同的文件，所以只能用集合Write锁
		StoragePackage().WriteAny().
		Storage().
		// 全量模式下删除对象文件。因为可能有多个User Move相同的文件，所以只能用集合Write锁
		WriteAnyPackage(t.StorageID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	var packages []model.StoragePackage
	for _, objID := range t.PackageIDs {
		objs, err := execCtx.Args.DB.StoragePackage().GetAllByStorageAndPackageID(execCtx.Args.DB.SQLCtx(), t.StorageID, objID)
		if err != nil {
			log.WithField("StorageID", t.StorageID).
				WithField("PackageID", objID).
				Warnf("get storage package failed, err: %s", err.Error())
			return
		}

		packages = append(packages, objs...)
	}

	t.startCheck(execCtx, stg, false, packages)
}

func (t *AgentCheckStorage) startCheck(execCtx ExecuteContext, stg model.Storage, isComplete bool, packages []model.StoragePackage) {
	log := logger.WithType[AgentCheckStorage]("Event")

	// 投递任务
	agtCli, err := stgglb.AgentMQPool.Acquire(stg.NodeID)
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	checkResp, err := agtCli.StorageCheck(agtmq.NewStorageCheck(stg.StorageID, stg.Directory, isComplete, packages), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("checking storage: %s", err.Error())
		return
	}

	// 根据返回结果修改数据库
	var chkObjIDs []cdssdk.PackageID
	for _, entry := range checkResp.Entries {
		switch entry.Operation {
		case agtmq.CHECK_STORAGE_RESP_OP_DELETE:
			err := execCtx.Args.DB.StoragePackage().Delete(execCtx.Args.DB.SQLCtx(), t.StorageID, entry.PackageID, entry.UserID)
			if err != nil {
				log.WithField("StorageID", t.StorageID).
					WithField("PackageID", entry.PackageID).
					Warnf("delete storage package failed, err: %s", err.Error())
			}
			chkObjIDs = append(chkObjIDs, entry.PackageID)

			log.WithField("StorageID", t.StorageID).
				WithField("PackageID", entry.PackageID).
				WithField("UserID", entry.UserID).
				Debugf("delete storage package")

		case agtmq.CHECK_STORAGE_RESP_OP_SET_NORMAL:
			err := execCtx.Args.DB.StoragePackage().SetStateNormal(execCtx.Args.DB.SQLCtx(), t.StorageID, entry.PackageID, entry.UserID)
			if err != nil {
				log.WithField("StorageID", t.StorageID).
					WithField("PackageID", entry.PackageID).
					Warnf("change storage package state failed, err: %s", err.Error())
			}

			log.WithField("StorageID", t.StorageID).
				WithField("PackageID", entry.PackageID).
				WithField("UserID", entry.UserID).
				Debugf("set storage package normal")
		}
	}

	if len(chkObjIDs) > 0 {
		execCtx.Executor.Post(NewCheckPackage(chkObjIDs))
	}
}

func init() {
	RegisterMessageConvertor(func(msg *scevt.AgentCheckStorage) Event { return NewAgentCheckStorage(msg.StorageID, msg.PackageIDs) })
}
