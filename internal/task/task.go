package task

import (
	"gitlink.org.cn/cloudream/common/pkg/task"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
)

type TaskContext struct {
	IPFS *ipfs.IPFS
}

// 需要在Task结束后主动调用，completing函数将在Manager加锁期间被调用，
// 因此适合进行执行结果的设置
type CompleteFn = task.CompleteFn

type Manager = task.Manager[TaskContext]

type TaskBody = task.TaskBody[TaskContext]

type Task = task.Task[TaskContext]

func NewManager(ipfs *ipfs.IPFS) Manager {
	return task.NewManager(TaskContext{
		IPFS: ipfs,
	})
}
