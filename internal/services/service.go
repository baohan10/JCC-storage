package services

import (
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
}

func NewService(coorClient *racli.Client, ipfsClient *ipfs.IPFS, scanner *sccli.Client, distlock *distlock.Service) (*Service, error) {
	return &Service{
		coordinator: coorClient,
		ipfs:        ipfsClient,
		scanner:     scanner,
		distlock:    distlock,
	}, nil
}
