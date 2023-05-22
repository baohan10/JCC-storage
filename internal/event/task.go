package event

import (
	event "gitlink.org.cn/cloudream/common/pkg/event"
	mydb "gitlink.org.cn/cloudream/db"
)

type ExecuteArgs struct {
	DB *mydb.DB
}

type Executor = event.Executor[ExecuteArgs]

type ExecuteContext = event.ExecuteContext[ExecuteArgs]

type Event = event.Event[ExecuteArgs]
