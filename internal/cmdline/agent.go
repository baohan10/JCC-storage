package cmdline

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkg/cmdtrie"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
	agtevt "gitlink.org.cn/cloudream/rabbitmq/message/agent/event"
)

var parseAgentEventCmdTrie cmdtrie.StaticCommandTrie[any]

func AgentPostEvent(c *Commandline, nodeID int, args []string) error {
	ret, err := parseAgentEventCmdTrie.Execute(args...)
	if err != nil {
		return fmt.Errorf("execute parsing event command failed, err: %w", err)
	}

	// TODO 支持设置标志
	err = c.Svc.AgentSvc().PostEvent(nodeID, ret, false, false)
	if err != nil {
		return fmt.Errorf("post event to agent failed, err: %w", err)
	}

	return nil
}

func init() {
	parseAgentEventCmdTrie.Add(agtevt.NewCheckCache, myreflect.TypeNameOf[agtevt.CheckCache]())

	parseAgentEventCmdTrie.Add(agtevt.NewCheckState, myreflect.TypeNameOf[agtevt.CheckState]())

	parseAgentEventCmdTrie.Add(agtevt.NewCheckStorage, myreflect.TypeNameOf[agtevt.CheckStorage]())

	commands.Add(ScannerPostEvent, "agent", "event")
}
