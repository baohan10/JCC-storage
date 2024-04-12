package services

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/client/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
)

type Service struct {
	DistLock   *distlock.Service
	TaskMgr    *task.Manager
	Downloader *downloader.Downloader
}

func NewService(distlock *distlock.Service, taskMgr *task.Manager, downloader *downloader.Downloader) (*Service, error) {
	return &Service{
		DistLock:   distlock,
		TaskMgr:    taskMgr,
		Downloader: downloader,
	}, nil
}
