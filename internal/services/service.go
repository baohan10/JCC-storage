package services

import (
	distlock "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	"gitlink.org.cn/cloudream/storage-client/internal/task"
	racli "gitlink.org.cn/cloudream/storage-common/pkgs/mq/client/coordinator"
	sccli "gitlink.org.cn/cloudream/storage-common/pkgs/mq/client/scanner"
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
