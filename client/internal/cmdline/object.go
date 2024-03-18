package cmdline

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

var _ = MustAddCmd(func(ctx CommandContext, packageID cdssdk.PackageID, rootPath string, nodeAffinity []cdssdk.NodeID) error {
	userID := cdssdk.UserID(1)

	var uploadFilePathes []string
	err := filepath.WalkDir(rootPath, func(fname string, fi os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if !fi.IsDir() {
			uploadFilePathes = append(uploadFilePathes, fname)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("open directory %s failed, err: %w", rootPath, err)
	}

	var nodeAff *cdssdk.NodeID
	if len(nodeAffinity) > 0 {
		n := cdssdk.NodeID(nodeAffinity[0])
		nodeAff = &n
	}

	objIter := iterator.NewUploadingObjectIterator(rootPath, uploadFilePathes)
	taskID, err := ctx.Cmdline.Svc.ObjectSvc().StartUploading(userID, packageID, objIter, nodeAff)
	if err != nil {
		return fmt.Errorf("update objects to package %d failed, err: %w", packageID, err)
	}

	for {
		complete, _, err := ctx.Cmdline.Svc.ObjectSvc().WaitUploading(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading objects: %w", err)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("wait updating: %w", err)
		}
	}
}, "obj", "upload")
