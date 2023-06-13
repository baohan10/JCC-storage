package cmd

import (
	"gitlink.org.cn/cloudream/agent/internal/task"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
)

type Service struct {
	ipfs        *ipfs.IPFS
	taskManager *task.Manager
}

func NewService(ipfs *ipfs.IPFS, taskMgr *task.Manager) *Service {
	return &Service{
		ipfs:        ipfs,
		taskManager: taskMgr,
	}
}
