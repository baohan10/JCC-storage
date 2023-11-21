package task

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type TaskContext struct {
	distlock *distlock.Service
	sw       *ioswitch.Switch
}

// 需要在Task结束后主动调用，completing函数将在Manager加锁期间被调用，
// 因此适合进行执行结果的设置
type CompleteFn = task.CompleteFn

type Manager = task.Manager[TaskContext]

type TaskBody = task.TaskBody[TaskContext]

type Task = task.Task[TaskContext]

type CompleteOption = task.CompleteOption

func NewManager(distlock *distlock.Service, sw *ioswitch.Switch) Manager {
	return task.NewManager(TaskContext{
		distlock: distlock,
		sw:       sw,
	})
}
