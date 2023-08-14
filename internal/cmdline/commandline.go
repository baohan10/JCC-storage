package cmdline

import (
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/common/pkgs/cmdtrie"
	distlocksvc "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	"gitlink.org.cn/cloudream/storage-client/internal/services"
)

type CommandContext struct {
	Cmdline *Commandline
}

var commands cmdtrie.CommandTrie[CommandContext, error] = cmdtrie.NewCommandTrie[CommandContext, error]()

type Commandline struct {
	Svc      *services.Service
	DistLock *distlocksvc.Service
	IPFS     *ipfs.IPFS
}

func NewCommandline(svc *services.Service, distLock *distlocksvc.Service, ipfs *ipfs.IPFS) (*Commandline, error) {
	return &Commandline{
		Svc:      svc,
		DistLock: distLock,
		IPFS:     ipfs,
	}, nil
}

func (c *Commandline) DispatchCommand(allArgs []string) {
	cmdCtx := CommandContext{
		Cmdline: c,
	}
	cmdErr, err := commands.Execute(cmdCtx, allArgs, cmdtrie.ExecuteOption{ReplaceEmptyArrayWithNil: true})
	if err != nil {
		fmt.Printf("execute command failed, err: %s", err.Error())
		os.Exit(1)
	}
	if cmdErr != nil {
		fmt.Printf("execute command failed, err: %s", cmdErr.Error())
		os.Exit(1)
	}
}
