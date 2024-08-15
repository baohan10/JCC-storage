package mq

import (
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
)

type Service struct {
	taskManager *task.Manager
}

func NewService(taskMgr *task.Manager) *Service {
	return &Service{
		taskManager: taskMgr,
	}
}
