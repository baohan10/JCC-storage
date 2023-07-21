package cmdline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/juju/ratelimit"
	myio "gitlink.org.cn/cloudream/common/utils/io"
)

func ObjectListBucketObjects(ctx CommandContext, bucketID int64) error {
	userID := int64(0)

	objects, err := ctx.Cmdline.Svc.BucketSvc().GetBucketObjects(userID, bucketID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d objects in bucket %d for user %d:\n", len(objects), bucketID, userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "Size", "BucketID", "State", "Redundancy"})

	for _, obj := range objects {
		tb.AppendRow(table.Row{obj.ObjectID, obj.Name, obj.FileSize, obj.BucketID, obj.State, obj.Redundancy})
	}

	fmt.Print(tb.Render())
	return nil
}

func ObjectDownloadObject(ctx CommandContext, localFilePath string, objectID int64) error {
	// 创建本地文件
	curExecPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable directory failed, err: %w", err)
	}

	outputFilePath := filepath.Join(filepath.Dir(curExecPath), localFilePath)
	outputFileDir := filepath.Dir(outputFilePath)

	err = os.MkdirAll(outputFileDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create output file directory %s failed, err: %w", outputFileDir, err)
	}

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("create output file %s failed, err: %w", outputFilePath, err)
	}
	defer outputFile.Close()

	// 下载文件
	reader, err := ctx.Cmdline.Svc.ObjectSvc().DownloadObject(0, objectID)
	if err != nil {
		return fmt.Errorf("download object failed, err: %w", err)
	}
	defer reader.Close()

	bkt := ratelimit.NewBucketWithRate(10*1024, 10*1024)
	_, err = io.Copy(outputFile, ratelimit.Reader(reader, bkt))
	if err != nil {
		return fmt.Errorf("copy object data to local file failed, err: %w", err)
	}

	return nil
}

func ObjectUploadRepObject(ctx CommandContext, localFilePath string, bucketID int64, objectName string, repCount int) error {
	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("open file %s failed, err: %w", localFilePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("get file %s state failed, err: %w", localFilePath, err)
	}
	fileSize := fileInfo.Size()

	// TODO 测试用
	bkt := ratelimit.NewBucketWithRate(10*1024, 10*1024)
	taskID, err := ctx.Cmdline.Svc.ObjectSvc().StartUploadingRepObject(0, bucketID, objectName,
		myio.WithCloser(ratelimit.Reader(file, bkt),
			func(reader io.Reader) error {
				return file.Close()
			}),
		fileSize, repCount)
	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	for {
		complete, fileHash, err := ctx.Cmdline.Svc.ObjectSvc().WaitUploadingRepObject(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading rep object: %w", err)
			}

			fmt.Print(fileHash)
			return nil
		}

		if err != nil {
			return fmt.Errorf("wait uploading: %w", err)
		}
	}
}

func ObjectEcWrite(ctx CommandContext, localFilePath string, bucketID int64, objectName string, ecName string) error {
	// TODO
	panic("not implement yet")
}

func ObjectUpdateRepObject(ctx CommandContext, objectID int64, filePath string) error {
	userID := int64(0)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file %s failed, err: %w", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("get file %s state failed, err: %w", filePath, err)
	}
	fileSize := fileInfo.Size()

	// TODO 测试用
	bkt := ratelimit.NewBucketWithRate(10*1024, 10*1024)
	taskID, err := ctx.Cmdline.Svc.ObjectSvc().StartUpdatingRepObject(userID, objectID,
		myio.WithCloser(ratelimit.Reader(file, bkt),
			func(reader io.Reader) error {
				return file.Close()
			}), fileSize)
	if err != nil {
		return fmt.Errorf("update object %d failed, err: %w", objectID, err)
	}

	for {
		complete, err := ctx.Cmdline.Svc.ObjectSvc().WaitUpdatingRepObject(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("updating rep object: %w", err)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("wait updating: %w", err)
		}
	}
}

func ObjectDeleteObject(ctx CommandContext, objectID int64) error {
	userID := int64(0)
	err := ctx.Cmdline.Svc.ObjectSvc().DeleteObject(userID, objectID)
	if err != nil {
		return fmt.Errorf("delete object %d failed, err: %w", objectID, err)
	}
	return nil
}

func init() {
	commands.MustAdd(ObjectListBucketObjects, "object", "ls")

	commands.MustAdd(ObjectUploadRepObject, "object", "new", "rep")

	commands.MustAdd(ObjectDownloadObject, "object", "get")

	commands.MustAdd(ObjectUpdateRepObject, "object", "update", "rep")

	commands.MustAdd(ObjectDeleteObject, "object", "delete")
}
