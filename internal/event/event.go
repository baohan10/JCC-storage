package event

import (
	"fmt"
	"reflect"

	event "gitlink.org.cn/cloudream/common/pkg/event"
	"gitlink.org.cn/cloudream/common/pkg/typedispatcher"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	mydb "gitlink.org.cn/cloudream/db"
	sccli "gitlink.org.cn/cloudream/rabbitmq/client/scanner"
)

type ExecuteArgs struct {
	Scanner *sccli.ScannerClient
	DB      *mydb.DB
	IPFS    *ipfs.IPFS
}

type Executor = event.Executor[ExecuteArgs]

type ExecuteContext = event.ExecuteContext[ExecuteArgs]

type Event = event.Event[ExecuteArgs]

type ExecuteOption = event.ExecuteOption

func NewExecutor(scanner *sccli.ScannerClient, db *mydb.DB, ipfs *ipfs.IPFS) Executor {
	return event.NewExecutor(ExecuteArgs{
		Scanner: scanner,
		DB:      db,
		IPFS:    ipfs,
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
