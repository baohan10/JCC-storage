package services

import (
	distlock "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	"gitlink.org.cn/cloudream/storage-client/internal/task"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
	scmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/scanner"
)

type Service struct {
	coordinator *coormq.Client
	ipfs        *ipfs.IPFS
	scanner     *scmq.Client
	distlock    *distlock.Service
	taskMgr     *task.Manager
}

func NewService(coorClient *coormq.Client, ipfsClient *ipfs.IPFS, scanner *scmq.Client, distlock *distlock.Service, taskMgr *task.Manager) (*Service, error) {
	return &Service{
		coordinator: coorClient,
		ipfs:        ipfsClient,
		scanner:     scanner,
		distlock:    distlock,
		taskMgr:     taskMgr,
	}, nil
}
