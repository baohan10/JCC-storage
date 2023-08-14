package cmd

import (
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	"gitlink.org.cn/cloudream/storage-agent/internal/task"
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
