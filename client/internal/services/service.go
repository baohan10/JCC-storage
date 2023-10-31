package services

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/client/internal/task"
)

type Service struct {
	DistLock *distlock.Service
	TaskMgr  *task.Manager
}

func NewService(distlock *distlock.Service, taskMgr *task.Manager) (*Service, error) {
	return &Service{
		DistLock: distlock,
		TaskMgr:  taskMgr,
	}, nil
}
