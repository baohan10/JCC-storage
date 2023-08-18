package task

import (
	distsvc "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

type TaskContext struct {
	ipfs        *ipfs.IPFS
	distLock    *distsvc.Service
	coordinator *coormq.Client
}

// 需要在Task结束后主动调用，completing函数将在Manager加锁期间被调用，
// 因此适合进行执行结果的设置
type CompleteFn = task.CompleteFn

type Manager = task.Manager[TaskContext]

type TaskBody = task.TaskBody[TaskContext]

type Task = task.Task[TaskContext]

type CompleteOption = task.CompleteOption

func NewManager(ipfs *ipfs.IPFS, distlock *distsvc.Service, coorCli *coormq.Client) Manager {
	return task.NewManager(TaskContext{
		ipfs:        ipfs,
		distLock:    distlock,
		coordinator: coorCli,
	})
}
