package task

type Task interface {
	TryMerge(other Task) bool // 尝试将other任务与自身合并，如果成功返回true
	Execute(ctx *ExecuteContext, myOpts ExecuteOption)
}
