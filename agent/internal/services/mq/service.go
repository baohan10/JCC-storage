package mq

import (
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Service struct {
	taskManager *task.Manager
	sw          *ioswitch.Switch
}

func NewService(taskMgr *task.Manager, sw *ioswitch.Switch) *Service {
	return &Service{
		taskManager: taskMgr,
		sw:          sw,
	}
}
