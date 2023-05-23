package cmd

import (
	"gitlink.org.cn/cloudream/agent/internal/event"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
)

type Service struct {
	ipfs          *ipfs.IPFS
	eventExecutor *event.Executor
}

func NewService(ipfs *ipfs.IPFS, eventExecutor *event.Executor) *Service {
	return &Service{
		ipfs:          ipfs,
		eventExecutor: eventExecutor,
	}
}
