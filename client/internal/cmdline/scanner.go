package cmdline

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/cmdtrie"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
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
	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCacheGC, reflect2.TypeNameOf[scevt.AgentCacheGC]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckCache, reflect2.TypeNameOf[scevt.AgentCheckCache]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckState, reflect2.TypeNameOf[scevt.AgentCheckState]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentStorageGC, reflect2.TypeNameOf[scevt.AgentStorageGC]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewAgentCheckStorage, reflect2.TypeNameOf[scevt.AgentCheckStorage]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewCheckPackage, reflect2.TypeNameOf[scevt.CheckPackage]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewCheckPackageRedundancy, reflect2.TypeNameOf[scevt.CheckPackageRedundancy]())

	parseScannerEventCmdTrie.MustAdd(scevt.NewCleanPinned, reflect2.TypeNameOf[scevt.CleanPinned]())

	commands.MustAdd(ScannerPostEvent, "scanner", "event")
}
