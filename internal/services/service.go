package services

import (
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	racli "gitlink.org.cn/cloudream/rabbitmq/client/coordinator"
	sccli "gitlink.org.cn/cloudream/rabbitmq/client/scanner"
)

type Service struct {
	coordinator *racli.Client
	ipfs        *ipfs.IPFS
	scanner     *sccli.Client
}

func NewService(coorClient *racli.Client, ipfsClient *ipfs.IPFS, scanner *sccli.Client) (*Service, error) {
	return &Service{
		coordinator: coorClient,
		ipfs:        ipfsClient,
		scanner:     scanner,
	}, nil
}
