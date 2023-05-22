package task

import (
	"context"
	"sync"

	"github.com/zyedidia/generic/list"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	mydb "gitlink.org.cn/cloudream/db"
	sccli "gitlink.org.cn/cloudream/rabbitmq/client/scanner"
	"golang.org/x/sync/semaphore"
)

type ExecuteOption struct {
	IsEmergency bool
	DontMerge   bool
}
type ExecuteContext struct {
	Executor *Executor
	Scanner  *sccli.ScannerClient
	DB       *mydb.DB
	IPFS     *ipfs.IPFS
}
type postedTask struct {
	Task   Task
	Option ExecuteOption
}

type Executor struct {
	tasks    list.List[postedTask]
	locker   sync.Mutex
	taskSema semaphore.Weighted
	execCtx  *ExecuteContext
}

func (e *Executor) Post(task Task, opts ...ExecuteOption) {
	opt := ExecuteOption{
		IsEmergency: false,
		DontMerge:   false,
	}

	if len(opts) > 0 {
		opt = opts[0]
	}

	e.locker.Lock()
	defer e.locker.Unlock()

	// 紧急任务直接插入到队头，不进行合并
	if opt.IsEmergency {
		e.tasks.PushFront(postedTask{
			Task:   task,
			Option: opt,
		})
		e.taskSema.Release(1)
		return
	}

	// 合并任务
	if opt.DontMerge {
		ptr := e.tasks.Front
		for ptr != nil {
			// 只与非紧急任务，且允许合并的任务进行合并
			if !ptr.Value.Option.IsEmergency && !ptr.Value.Option.DontMerge {
				if ptr.Value.Task.TryMerge(task) {
					return
				}
			}

			ptr = ptr.Next
		}
	}

	e.tasks.PushBack(postedTask{
		Task:   task,
		Option: opt,
	})
	e.taskSema.Release(1)
}

// Execute 开始执行任务
func (e *Executor) Execute() error {
	for {
		// TODO 打印错误日志
		e.taskSema.Acquire(context.Background(), 1)

		task := e.popFrontTask()
		if task == nil {
			continue
		}

		task.Task.Execute(e.execCtx, task.Option)
	}
}

func (e *Executor) popFrontTask() *postedTask {
	e.locker.Lock()
	defer e.locker.Unlock()

	if e.tasks.Front == nil {
		return nil
	}

	return &e.tasks.Front.Value
}
