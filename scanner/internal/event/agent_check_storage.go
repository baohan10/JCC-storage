package event

import (
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type AgentCheckStorage struct {
	*scevt.AgentCheckStorage
}

func NewAgentCheckStorage(evt *scevt.AgentCheckStorage) *AgentCheckStorage {
	return &AgentCheckStorage{
		AgentCheckStorage: evt,
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

	if node.State != consts.NodeStateNormal {
		return
	}

	agtCli, err := stgglb.AgentMQPool.Acquire(stg.NodeID)
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	checkResp, err := agtCli.StorageCheck(agtmq.NewStorageCheck(stg.StorageID, stg.Directory), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("NodeID", stg.NodeID).Warnf("checking storage: %s", err.Error())
		return
	}
	realPkgs := make(map[cdssdk.UserID]map[cdssdk.PackageID]bool)
	for _, pkg := range checkResp.Packages {
		pkgs, ok := realPkgs[pkg.UserID]
		if !ok {
			pkgs = make(map[cdssdk.PackageID]bool)
			realPkgs[pkg.UserID] = pkgs
		}

		pkgs[pkg.PackageID] = true
	}

	execCtx.Args.DB.DoTx(sql.LevelLinearizable, func(tx *sqlx.Tx) error {
		packages, err := execCtx.Args.DB.StoragePackage().GetAllByStorageID(tx, t.StorageID)
		if err != nil {
			log.Warnf("getting storage package: %s", err.Error())
			return nil
		}

		var rms []model.StoragePackage
		for _, pkg := range packages {
			pkgMap, ok := realPkgs[pkg.UserID]
			if !ok {
				rms = append(rms, pkg)
				continue
			}

			if !pkgMap[pkg.PackageID] {
				rms = append(rms, pkg)
			}
		}

		rmdPkgIDs := make(map[cdssdk.PackageID]bool)
		for _, rm := range rms {
			err := execCtx.Args.DB.StoragePackage().Delete(tx, rm.StorageID, rm.PackageID, rm.UserID)
			if err != nil {
				log.Warnf("deleting storage package: %s", err.Error())
				continue
			}
			rmdPkgIDs[rm.PackageID] = true
		}

		// 彻底删除已经是Deleted状态，且不被再引用的Package
		for pkgID := range rmdPkgIDs {
			err := execCtx.Args.DB.Package().DeleteUnused(tx, pkgID)
			if err != nil {
				log.Warnf("deleting unused package: %s", err.Error())
				continue
			}
		}

		return nil
	})
}

func init() {
	RegisterMessageConvertor(NewAgentCheckStorage)
}
