package cmd

import "gitlink.org.cn/cloudream/common/utils/ipfs"

type Service struct {
	ipfs *ipfs.IPFS
}

func NewService(ipfs *ipfs.IPFS) *Service {
	return &Service{
		ipfs: ipfs,
	}
}
