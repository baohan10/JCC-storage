package cmdline

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/cmdtrie"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

var parseScannerEventCmdTrie cmdtrie.StaticCommandTrie[any] = cmdtrie.NewStaticCommandTrie[any]()

func ScannerPostEvent(ctx CommandContext, args []string) error {
	ret, err := parseScannerEventCmdTrie.Execute(args, cmdtrie.ExecuteOption{ReplaceEmptyArrayWithNil: true})
	if err != nil {
		return fmt.Errorf("execute parsing event command failed, err: %w", err)
	}

	err = ctx.Cmdline.Svc.ScannerSvc().PostEvent(ret.(scevt.Event), false, false)
	if err != nil {
		return fmt.Errorf("post event to scanner failed, err: %w", err)
	}

	return nil
}

func init() {
	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCacheGC, myreflect.TypeNameOf[scevt.AgentCacheGC]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckCache, myreflect.TypeNameOf[scevt.AgentCheckCache]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckState, myreflect.TypeNameOf[scevt.AgentCheckState]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentStorageGC, myreflect.TypeNameOf[scevt.AgentStorageGC]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckStorage, myreflect.TypeNameOf[scevt.AgentCheckStorage]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewCheckPackage, myreflect.TypeNameOf[scevt.CheckPackage]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewCheckPackageRedundancy, myreflect.TypeNameOf[scevt.CheckPackageRedundancy]())

	commands.MustAdd(ScannerPostEvent, "scanner", "event")
}
