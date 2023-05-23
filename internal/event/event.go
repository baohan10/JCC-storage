package event

import (
	"fmt"
	"reflect"

	event "gitlink.org.cn/cloudream/common/pkg/event"
	"gitlink.org.cn/cloudream/common/pkg/typedispatcher"
	mydb "gitlink.org.cn/cloudream/db"
)

type ExecuteArgs struct {
	DB *mydb.DB
}

type Executor = event.Executor[ExecuteArgs]

type ExecuteContext = event.ExecuteContext[ExecuteArgs]

type Event = event.Event[ExecuteArgs]

type ExecuteOption = event.ExecuteOption

func NewExecutor(db *mydb.DB) Executor {
	return event.NewExecutor(ExecuteArgs{
		DB: db,
	})
}

var msgDispatcher typedispatcher.TypeDispatcher[Event]

func FromMessage(msg any) (Event, error) {
	event, ok := msgDispatcher.Dispatch(msg)
	if !ok {
		return nil, fmt.Errorf("unknow event message type: %s", reflect.TypeOf(msg).Name())
	}

	return event, nil
}

func Register[T any](converter func(msg T) Event) {
	typedispatcher.Add(msgDispatcher, converter)
}
