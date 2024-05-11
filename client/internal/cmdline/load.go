package cmdline

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func init() {
	var useID bool
	cmd := cobra.Command{
		Use:   "load",
		Short: "Load data from CDS to a storage service",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			cmdCtx := GetCmdCtx(cmd)

			if useID {
				pkgID, err := strconv.ParseInt(args[0], 10, 64)
				if err != nil {
					fmt.Printf("Invalid package ID: %s\n", args[0])
				}

				stgID, err := strconv.ParseInt(args[1], 10, 64)
				if err != nil {
					fmt.Printf("Invalid storage ID: %s\n", args[1])
				}

				loadByID(cmdCtx, cdssdk.PackageID(pkgID), cdssdk.StorageID(stgID))
			} else {
				loadByPath(cmdCtx, args[0], args[1])
			}
		},
	}
	cmd.Flags().BoolVarP(&useID, "id", "i", false, "Use ID for both package and storage service instead of their name or path")
	rootCmd.AddCommand(&cmd)
}

func loadByPath(cmdCtx *CommandContext, pkgPath string, stgName string) {
	userID := cdssdk.UserID(1)

	comps := strings.Split(strings.Trim(pkgPath, cdssdk.ObjectPathSeparator), cdssdk.ObjectPathSeparator)
	if len(comps) != 2 {
		fmt.Printf("Package path must be in format of <bucket>/<package>")
		return
	}

	pkg, err := cmdCtx.Cmdline.Svc.PackageSvc().GetByName(userID, comps[0], comps[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	stg, err := cmdCtx.Cmdline.Svc.StorageSvc().GetByName(userID, stgName)
	if err != nil {
		fmt.Println(err)
		return
	}

	loadByID(cmdCtx, pkg.PackageID, stg.StorageID)
}

func loadByID(cmdCtx *CommandContext, pkgID cdssdk.PackageID, stgID cdssdk.StorageID) {
	userID := cdssdk.UserID(1)
	startTime := time.Now()

	nodeID, taskID, err := cmdCtx.Cmdline.Svc.StorageSvc().StartStorageLoadPackage(userID, pkgID, stgID)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		complete, fullPath, err := cmdCtx.Cmdline.Svc.StorageSvc().WaitStorageLoadPackage(nodeID, taskID, time.Second*10)
		if err != nil {
			fmt.Println(err)
			return
		}

		if complete {
			fmt.Printf("Package loaded to: %s in %v\n", fullPath, time.Since(startTime))
			break
		}
	}
}
