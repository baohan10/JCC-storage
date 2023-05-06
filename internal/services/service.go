package services

import (
	racli "gitlink.org.cn/cloudream/rabbitmq/client/coordinator"
	"gitlink.org.cn/cloudream/utils/ipfs"
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
