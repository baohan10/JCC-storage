package cmd

import (
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	"gitlink.org.cn/cloudream/storage-agent/internal/task"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

type Service struct {
	ipfs        *ipfs.IPFS
	taskManager *task.Manager
	coordinator *coormq.Client
}

func NewService(ipfs *ipfs.IPFS, taskMgr *task.Manager, coordinator *coormq.Client) *Service {
	return &Service{
		ipfs:        ipfs,
		taskManager: taskMgr,
		coordinator: coordinator,
	}
}
