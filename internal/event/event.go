package event

import (
	"fmt"
	"reflect"

	distlocksvc "gitlink.org.cn/cloudream/common/pkg/distlock/service"
	event "gitlink.org.cn/cloudream/common/pkg/event"
	"gitlink.org.cn/cloudream/common/pkg/typedispatcher"
	mydb "gitlink.org.cn/cloudream/db"
)

type ExecuteArgs struct {
	DB       *mydb.DB
	DistLock *distlocksvc.Service
}

type Executor = event.Executor[ExecuteArgs]

type ExecuteContext = event.ExecuteContext[ExecuteArgs]

type Event = event.Event[ExecuteArgs]

type ExecuteOption = event.ExecuteOption

func NewExecutor(db *mydb.DB, distLock *distlocksvc.Service) Executor {
	return event.NewExecutor(ExecuteArgs{
		DB:       db,
		DistLock: distLock,
	})
}

var msgDispatcher = typedispatcher.NewTypeDispatcher[Event]()

func FromMessage(msg any) (Event, error) {
	event, ok := msgDispatcher.Dispatch(msg)
	if !ok {
		return nil, fmt.Errorf("unknow event message type: %s", reflect.TypeOf(msg).Name())
	}

	return event, nil
}

func RegisterMessageConvertor[T any](converter func(msg T) Event) {
	typedispatcher.Add(msgDispatcher, converter)
}
