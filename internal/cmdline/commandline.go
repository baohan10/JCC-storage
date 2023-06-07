package cmdline

import (
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/client/internal/services"
	"gitlink.org.cn/cloudream/common/pkg/cmdtrie"
)

type CommandContext struct {
	Cmdline *Commandline
}

var commands cmdtrie.CommandTrie[CommandContext, error] = cmdtrie.NewCommandTrie[CommandContext, error]()

type Commandline struct {
	Svc *services.Service
}

func NewCommandline(svc *services.Service) (*Commandline, error) {
	return &Commandline{
		Svc: svc,
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
