package cmdline

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/inhies/go-bytesize"
	"github.com/spf13/cobra"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

func init() {
	var nodeID int64
	cmd := &cobra.Command{
		Use:   "put",
		Short: "Upload files to CDS",
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(2)(cmd, args); err != nil {
				return err
			}

			remote := args[1]
			comps := strings.Split(strings.Trim(remote, cdssdk.ObjectPathSeparator), cdssdk.ObjectPathSeparator)
			if len(comps) != 2 {
				return fmt.Errorf("invalid remote path: %s, which must be in format of <bucket>/<package>", remote)
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			userID := cdssdk.UserID(1)
			cmdCtx := GetCmdCtx(cmd)

			local := args[0]
			remote := args[1]
			comps := strings.Split(strings.Trim(remote, cdssdk.ObjectPathSeparator), cdssdk.ObjectPathSeparator)

			startTime := time.Now()

			bkt, err := cmdCtx.Cmdline.Svc.BucketSvc().GetBucketByName(userID, comps[0])
			if err != nil {
				fmt.Printf("getting bucket: %v\n", err)
				return
			}

			pkg, err := cmdCtx.Cmdline.Svc.PackageSvc().GetByName(userID, comps[0], comps[1])
			if err != nil {
				if codeMsg, ok := err.(*mq.CodeMessageError); ok && codeMsg.Code == errorcode.DataNotFound {
					pkg2, err := cmdCtx.Cmdline.Svc.PackageSvc().Create(userID, bkt.BucketID, comps[1])
					if err != nil {
						fmt.Printf("creating package: %v\n", err)
						return
					}
					pkg = &pkg2

				} else {
					fmt.Printf("getting package: %v\n", err)
					return
				}
			}

			var fileCount int
			var totalSize int64
			var uploadFilePathes []string
			err = filepath.WalkDir(local, func(fname string, fi os.DirEntry, err error) error {
				if err != nil {
					return nil
				}

				if !fi.IsDir() {
					uploadFilePathes = append(uploadFilePathes, fname)
					fileCount++

					info, err := fi.Info()
					if err == nil {
						totalSize += info.Size()
					}
				}

				return nil
			})
			if err != nil {
				fmt.Printf("walking directory: %v\n", err)
				return
			}

			var nodeAff *cdssdk.NodeID
			if nodeID != 0 {
				id := cdssdk.NodeID(nodeID)
				nodeAff = &id
			}

			objIter := iterator.NewUploadingObjectIterator(local, uploadFilePathes)
			taskID, err := cmdCtx.Cmdline.Svc.ObjectSvc().StartUploading(userID, pkg.PackageID, objIter, nodeAff)
			if err != nil {
				fmt.Printf("start uploading objects: %v\n", err)
				return
			}

			for {
				complete, _, err := cmdCtx.Cmdline.Svc.ObjectSvc().WaitUploading(taskID, time.Second*5)
				if err != nil {
					fmt.Printf("uploading objects: %v\n", err)
					return
				}

				if complete {
					break
				}
			}

			fmt.Printf("Put %v files (%v) to %s in %v.\n", fileCount, bytesize.ByteSize(totalSize), remote, time.Since(startTime))
		},
	}
	cmd.Flags().Int64VarP(&nodeID, "node", "n", 0, "node affinity")

	rootCmd.AddCommand(cmd)
}
