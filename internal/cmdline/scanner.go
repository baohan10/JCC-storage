package cmdline

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkg/cmdtrie"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

var parseScannerEventCmdTrie cmdtrie.StaticCommandTrie[any] = cmdtrie.NewStaticCommandTrie[any]()

func ScannerPostEvent(ctx CommandContext, args []string) error {
	ret, err := parseScannerEventCmdTrie.Execute(args, cmdtrie.ExecuteOption{ReplaceEmptyArrayWithNil: true})
	if err != nil {
		return fmt.Errorf("execute parsing event command failed, err: %w", err)
	}

	err = ctx.Cmdline.Svc.ScannerSvc().PostEvent(ret, false, false)
	if err != nil {
		return fmt.Errorf("post event to scanner failed, err: %w", err)
	}

	return nil
}

func init() {
	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckCache, myreflect.TypeNameOf[scevt.AgentCheckCache]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckState, myreflect.TypeNameOf[scevt.AgentCheckState]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckStorage, myreflect.TypeNameOf[scevt.AgentCheckStorage]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewCheckCache, myreflect.TypeNameOf[scevt.CheckCache]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewCheckObject, myreflect.TypeNameOf[scevt.CheckObject]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewCheckRepCount, myreflect.TypeNameOf[scevt.CheckRepCount]())

	commands.MustAdd(ScannerPostEvent, "scanner", "event")
}
