package task

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/downloader"
)

type TaskContext struct {
	distlock     *distlock.Service
	connectivity *connectivity.Collector
	downloader   *downloader.Downloader
}

// 需要在Task结束后主动调用，completing函数将在Manager加锁期间被调用，
// 因此适合进行执行结果的设置
type CompleteFn = task.CompleteFn

type Manager = task.Manager[TaskContext]

type TaskBody = task.TaskBody[TaskContext]

type Task = task.Task[TaskContext]

type CompleteOption = task.CompleteOption

func NewManager(distlock *distlock.Service, connectivity *connectivity.Collector, downloader *downloader.Downloader) Manager {
	return task.NewManager(TaskContext{
		distlock:     distlock,
		connectivity: connectivity,
		downloader:   downloader,
	})
}
