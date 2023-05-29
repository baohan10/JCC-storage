package cmdline

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkg/cmdtrie"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

var parseScannerEventCmdTrie cmdtrie.StaticCommandTrie[any]

func ScannerPostEvent(c *Commandline, args []string) error {
	ret, err := parseScannerEventCmdTrie.Execute(args...)
	if err != nil {
		return fmt.Errorf("execute parsing event command failed, err: %w", err)
	}

	// TODO 支持设置标志
	err = c.Svc.ScannerSvc().PostEvent(ret, false, false)
	if err != nil {
		return fmt.Errorf("post event to scanner failed, err: %w", err)
	}

	return nil
}

func init() {
	parseScannerEventCmdTrie.Add(scevt.NewAgentCheckCache, myreflect.TypeNameOf[scevt.AgentCheckCache]())

	parseScannerEventCmdTrie.Add(scevt.NewAgentCheckState, myreflect.TypeNameOf[scevt.AgentCheckState]())

	parseScannerEventCmdTrie.Add(scevt.NewAgentCheckStorage, myreflect.TypeNameOf[scevt.AgentCheckStorage]())

	parseScannerEventCmdTrie.Add(scevt.NewCheckCache, myreflect.TypeNameOf[scevt.CheckObject]())

	parseScannerEventCmdTrie.Add(scevt.NewCheckObject, myreflect.TypeNameOf[scevt.CheckCache]())

	parseScannerEventCmdTrie.Add(scevt.NewCheckRepCount, myreflect.TypeNameOf[scevt.CheckRepCount]())

	commands.Add(ScannerPostEvent, "scanner", "event")
}
