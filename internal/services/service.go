package services

import (
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	racli "gitlink.org.cn/cloudream/rabbitmq/client/coordinator"
	sccli "gitlink.org.cn/cloudream/rabbitmq/client/scanner"
)

type Service struct {
	coordinator *racli.CoordinatorClient
	ipfs        *ipfs.IPFS
	scanner     *sccli.ScannerClient
}

func NewService(coorClient *racli.CoordinatorClient, ipfsClient *ipfs.IPFS, scanner *sccli.ScannerClient) (*Service, error) {
	return &Service{
		coordinator: coorClient,
		ipfs:        ipfsClient,
		scanner:     scanner,
	}, nil
}
