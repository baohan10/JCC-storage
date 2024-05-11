package cmdline

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gitlink.org.cn/cloudream/common/pkgs/cmdtrie"
	"gitlink.org.cn/cloudream/storage/client/internal/services"
)

type CommandContext struct {
	Cmdline *Commandline
}

// TODO 逐步使用cobra代替cmdtrie
var commands cmdtrie.CommandTrie[CommandContext, error] = cmdtrie.NewCommandTrie[CommandContext, error]()

var rootCmd = cobra.Command{}

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
		if err == cmdtrie.ErrCommandNotFound {
			ctx := context.WithValue(context.Background(), "cmdCtx", &cmdCtx)
			err = rootCmd.ExecuteContext(ctx)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			return
		}

		fmt.Printf("execute command failed, err: %s", err.Error())
		os.Exit(1)
	}
	if cmdErr != nil {
		fmt.Printf("execute command failed, err: %s", cmdErr.Error())
		os.Exit(1)
	}
}

func MustAddCmd(fn any, prefixWords ...string) any {
	commands.MustAdd(fn, prefixWords...)
	return nil
}

func GetCmdCtx(cmd *cobra.Command) *CommandContext {
	return cmd.Context().Value("cmdCtx").(*CommandContext)
}
