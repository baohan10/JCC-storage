package cmdline

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func init() {
	var usePkgID *bool
	cmd := &cobra.Command{
		Use:   "lsp",
		Short: "List package information",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdCtx := GetCmdCtx(cmd)

			if usePkgID != nil && *usePkgID {
				id, err := strconv.ParseInt(args[0], 10, 64)
				if err != nil {
					fmt.Printf("Invalid package id: %s\n", args[0])
					return
				}

				lspOneByID(cmdCtx, cdssdk.PackageID(id))
			} else {
				lspByPath(cmdCtx, args[0])
			}
		},
	}
	usePkgID = cmd.Flags().BoolP("id", "i", false, "List with package id instead of path")

	rootCmd.AddCommand(cmd)
}

func lspByPath(cmdCtx *CommandContext, path string) {
	userID := cdssdk.UserID(1)

	comps := strings.Split(strings.Trim(path, cdssdk.ObjectPathSeparator), cdssdk.ObjectPathSeparator)
	if len(comps) != 2 {
		fmt.Printf("Package path must be in format of <bucket>/<package>")
		return
	}

	pkg, err := cmdCtx.Cmdline.Svc.PackageSvc().GetByName(userID, comps[0], comps[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	wr := table.NewWriter()
	wr.AppendHeader(table.Row{"ID", "Name", "State"})
	wr.AppendRow(table.Row{pkg.PackageID, pkg.Name, pkg.State})
	fmt.Println(wr.Render())
}

func lspOneByID(cmdCtx *CommandContext, id cdssdk.PackageID) {
	userID := cdssdk.UserID(1)

	pkg, err := cmdCtx.Cmdline.Svc.PackageSvc().Get(userID, id)
	if err != nil {
		fmt.Println(err)
		return
	}

	wr := table.NewWriter()
	wr.AppendHeader(table.Row{"ID", "Name", "State"})
	wr.AppendRow(table.Row{pkg.PackageID, pkg.Name, pkg.State})
	fmt.Println(wr.Render())
}
