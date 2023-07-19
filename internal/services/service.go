package services

import (
	"gitlink.org.cn/cloudream/client/internal/task"
	distlock "gitlink.org.cn/cloudream/common/pkg/distlock/service"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	racli "gitlink.org.cn/cloudream/rabbitmq/client/coordinator"
	sccli "gitlink.org.cn/cloudream/rabbitmq/client/scanner"
)

type Service struct {
	coordinator *racli.Client
	ipfs        *ipfs.IPFS
	scanner     *sccli.Client
	distlock    *distlock.Service
	taskMgr     *task.Manager
}

func NewService(coorClient *racli.Client, ipfsClient *ipfs.IPFS, scanner *sccli.Client, distlock *distlock.Service, taskMgr *task.Manager) (*Service, error) {
	return &Service{
		coordinator: coorClient,
		ipfs:        ipfsClient,
		scanner:     scanner,
		distlock:    distlock,
		taskMgr:     taskMgr,
	}, nil
}
