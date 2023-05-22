package services

import (
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	racli "gitlink.org.cn/cloudream/rabbitmq/client/coordinator"
)

type Service struct {
	coordinator *racli.CoordinatorClient
	ipfs        *ipfs.IPFS
}

func NewService(coorClient *racli.CoordinatorClient, ipfsClient *ipfs.IPFS) (*Service, error) {
	return &Service{
		coordinator: coorClient,
		ipfs:        ipfsClient,
	}, nil
}
