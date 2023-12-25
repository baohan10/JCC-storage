package event

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"

	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type AgentStorageGC struct {
	*scevt.AgentStorageGC
}

func NewAgentStorageGC(evt *scevt.AgentStorageGC) *AgentStorageGC {
	return &AgentStorageGC{
		AgentStorageGC: evt,
	}
}

func (t *AgentStorageGC) TryMerge(other Event) bool {
	event, ok := other.(*AgentStorageGC)
	if !ok {
		return false
	}

	if event.StorageID != t.StorageID {
		return false
	}

	return true
}

func (t *AgentStorageGC) Execute(execCtx ExecuteContext) {
	log := logger.WithType[AgentStorageGC]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t.AgentStorageGC))
	defer log.Debugf("end")

	// TODO unavailable的节点需不需要发送任务？

	mutex, err := reqbuilder.NewBuilder().
		// 进行GC
		Storage().GC(t.StorageID).
		MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	getStg, err := execCtx.Args.DB.Storage().GetByID(execCtx.Args.DB.SQLCtx(), t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("getting storage: %s", err.Error())
		return
	}

	stgPkgs, err := execCtx.Args.DB.StoragePackage().GetAllByStorageID(execCtx.Args.DB.SQLCtx(), t.StorageID)
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("getting storage packages: %s", err.Error())
		return
	}

	agtCli, err := stgglb.AgentMQPool.Acquire(getStg.NodeID)
	if err != nil {
		log.WithField("NodeID", getStg.NodeID).Warnf("create agent client failed, err: %s", err.Error())
		return
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	_, err = agtCli.StorageGC(agtmq.ReqStorageGC(t.StorageID, getStg.Directory, stgPkgs), mq.RequestOption{Timeout: time.Minute})
	if err != nil {
		log.WithField("StorageID", t.StorageID).Warnf("storage gc: %s", err.Error())
		return
	}
}

func init() {
	RegisterMessageConvertor(NewAgentStorageGC)
}
