package cmdline

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkg/cmdtrie"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
	agtevt "gitlink.org.cn/cloudream/rabbitmq/message/agent/event"
)

var parseAgentEventCmdTrie cmdtrie.StaticCommandTrie[any] = cmdtrie.NewStaticCommandTrie[any]()

func AgentPostEvent(ctx CommandContext, nodeID int, args []string) error {
	ret, err := parseAgentEventCmdTrie.Execute(args, cmdtrie.ExecuteOption{ReplaceEmptyArrayWithNil: true})
	if err != nil {
		return fmt.Errorf("execute parsing event command failed, err: %w", err)
	}

	// TODO 支持设置标志
	err = ctx.Cmdline.Svc.AgentSvc().PostEvent(nodeID, ret, false, false)
	if err != nil {
		return fmt.Errorf("post event to agent failed, err: %w", err)
	}

	return nil
}

func init() {
	parseAgentEventCmdTrie.MustAdd(agtevt.NewCheckCache, myreflect.TypeNameOf[agtevt.CheckCache]())

	parseAgentEventCmdTrie.MustAdd(agtevt.NewCheckState, myreflect.TypeNameOf[agtevt.CheckState]())

	parseAgentEventCmdTrie.MustAdd(agtevt.NewCheckStorage, myreflect.TypeNameOf[agtevt.CheckStorage]())

	commands.MustAdd(AgentPostEvent, "agent", "event")
}
