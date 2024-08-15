package grpc

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	agentserver "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
)

type Service struct {
	agentserver.AgentServer
	swWorker *exec.Worker
}

func NewService(swWorker *exec.Worker) *Service {
	return &Service{
		swWorker: swWorker,
	}
}
