package grpc

import (
	agentserver "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Service struct {
	agentserver.AgentServer
	swMgr *ioswitch.Manager
}

func NewService(swMgr *ioswitch.Manager) *Service {
	return &Service{
		swMgr: swMgr,
	}
}
