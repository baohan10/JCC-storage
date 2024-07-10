package mq

import (
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Service struct {
	taskManager *task.Manager
	swMgr       *ioswitch.Manager
}

func NewService(taskMgr *task.Manager, swMgr *ioswitch.Manager) *Service {
	return &Service{
		taskManager: taskMgr,
		swMgr:       swMgr,
	}
}
