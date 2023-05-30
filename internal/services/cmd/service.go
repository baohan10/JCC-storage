package cmd

import (
	"gitlink.org.cn/cloudream/agent/internal/event"
	"gitlink.org.cn/cloudream/agent/internal/task"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
)

type Service struct {
	ipfs          *ipfs.IPFS
	eventExecutor *event.Executor
	taskManager   *task.Manager
}

func NewService(ipfs *ipfs.IPFS, eventExecutor *event.Executor, taskMgr *task.Manager) *Service {
	return &Service{
		ipfs:          ipfs,
		eventExecutor: eventExecutor,
		taskManager:   taskMgr,
	}
}
