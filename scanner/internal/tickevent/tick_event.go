package tickevent

import (
	tickevent "gitlink.org.cn/cloudream/common/pkgs/tickevent"
	mydb "gitlink.org.cn/cloudream/storage/common/pkgs/db"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
)

type ExecuteArgs struct {
	EventExecutor *event.Executor
	DB            *mydb.DB
}

type StartOption = tickevent.StartOption

type Executor = tickevent.Executor[ExecuteArgs]

type ExecuteContext = tickevent.ExecuteContext[ExecuteArgs]

type Event = tickevent.TickEvent[ExecuteArgs]

func NewExecutor(args ExecuteArgs) Executor {
	return tickevent.NewExecutor(args)
}
