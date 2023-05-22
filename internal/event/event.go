package event

import (
	event "gitlink.org.cn/cloudream/common/pkg/event"
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
