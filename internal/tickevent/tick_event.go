package tickevent

import (
	tickevent "gitlink.org.cn/cloudream/common/pkg/tickevent"
	mydb "gitlink.org.cn/cloudream/db"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

type ExecuteArgs struct {
	EventExecutor *event.Executor
	DB            *mydb.DB
}

type Executor = tickevent.Executor[ExecuteArgs]

type ExecuteContext = tickevent.ExecuteContext[ExecuteArgs]

type Event = tickevent.TickEvent[ExecuteArgs]

func NewExecutor(args ExecuteArgs) Executor {
	return tickevent.NewExecutor(args)
}
