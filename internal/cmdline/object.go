package cmdline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"gitlink.org.cn/cloudream/storage-client/internal/task"
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
	outputFileDir := filepath.Dir(localFilePath)

	err := os.MkdirAll(outputFileDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create output file directory %s failed, err: %w", outputFileDir, err)
	}

	outputFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("create output file %s failed, err: %w", localFilePath, err)
	}
	defer outputFile.Close()

	// 下载文件
	reader, err := ctx.Cmdline.Svc.ObjectSvc().DownloadObject(0, objectID)
	if err != nil {
		return fmt.Errorf("download object failed, err: %w", err)
	}
	defer reader.Close()

	_, err = io.Copy(outputFile, reader)
	if err != nil {
		return fmt.Errorf("copy object data to local file failed, err: %w", err)
	}

	return nil
}

func ObjectDownloadObjectDir(ctx CommandContext, outputBaseDir string, dirName string) error {
	// 创建本地文件夹
	err := os.MkdirAll(outputBaseDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create output base directory %s failed, err: %w", outputBaseDir, err)
	}

	// 下载文件夹
	resObjs, err := ctx.Cmdline.Svc.ObjectSvc().DownloadObjectDir(0, dirName)
	if err != nil {
		return fmt.Errorf("download folder failed, err: %w", err)
	}

	// 遍历 关闭文件流
	defer func() {
		for _, resObj := range resObjs {
			resObj.Reader.Close()
		}
	}()

	for i := 0; i < len(resObjs); i++ {
		if resObjs[i].Error != nil {
			fmt.Printf("download file %s failed, err: %s", resObjs[i].ObjectName, err.Error())
			continue
		}
		outputFilePath := filepath.Join(outputBaseDir, resObjs[i].ObjectName)
		outputFileDir := filepath.Dir(outputFilePath)
		err = os.MkdirAll(outputFileDir, os.ModePerm)
		if err != nil {
			fmt.Printf("create output file directory %s failed, err: %s", outputFileDir, err.Error())
			continue
		}

		outputFile, err := os.Create(outputFilePath)
		if err != nil {
			fmt.Printf("create output file %s failed, err: %s", outputFilePath, err.Error())
			continue
		}
		defer outputFile.Close()

		_, err = io.Copy(outputFile, resObjs[i].Reader)
		if err != nil {
			// TODO 写入到文件失败，是否要考虑删除这个不完整的文件？
			fmt.Printf("copy object data to local file failed, err: %s", err.Error())
			continue
		}
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

	uploadObject := task.UploadObject{
		ObjectName: objectName,
		File:       file,
		FileSize:   fileSize,
	}
	uploadObjects := []task.UploadObject{uploadObject}

	taskID, err := ctx.Cmdline.Svc.ObjectSvc().StartUploadingRepObjects(0, bucketID, uploadObjects, repCount)
	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	for {
		complete, uploadObjectResult, err := ctx.Cmdline.Svc.ObjectSvc().WaitUploadingRepObjects(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading rep object: %w", err)
			}

			uploadRet := uploadObjectResult.Results[0]
			if uploadRet.Error != nil {
				return uploadRet.Error
			}

			fmt.Print(uploadRet.FileHash)
			return nil
		}

		if err != nil {
			return fmt.Errorf("wait uploading: %w", err)
		}
	}
}

func ObjectUploadEcObject(ctx CommandContext, localFilePath string, bucketID int64, objectName string, ecName string) error {
	// TODO 参考rep的，改成异步流程
	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("open file %s failed, err: %w", localFilePath, err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("get file %s state failed, err: %w", localFilePath, err)
	}
	fileSize := fileInfo.Size()

	err = ctx.Cmdline.Svc.ObjectSvc().UploadEcObject(0, bucketID, objectName, file, fileSize, ecName)
	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	return nil
}

func ObjectUploadRepObjectDir(ctx CommandContext, localDirPath string, bucketID int64, repCount int) error {
	var uploadFiles []task.UploadObject
	var uploadFile task.UploadObject
	err := filepath.Walk(localDirPath, func(fname string, fi os.FileInfo, err error) error {
		if !fi.IsDir() {
			file, err := os.Open(fname)
			if err != nil {
				return fmt.Errorf("open file %s failed, err: %w", fname, err)
			}
			uploadFile = task.UploadObject{
				ObjectName: filepath.ToSlash(fname),
				File:       file,
				FileSize:   fi.Size(),
			}
			uploadFiles = append(uploadFiles, uploadFile)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("open directory %s failed, err: %w", localDirPath, err)
	}

	// 遍历 关闭文件流
	defer func() {
		for _, uploadFile := range uploadFiles {
			uploadFile.File.Close()
		}
	}()

	taskID, err := ctx.Cmdline.Svc.ObjectSvc().StartUploadingRepObjects(0, bucketID, uploadFiles, repCount)

	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	for {
		complete, uploadObjectResult, err := ctx.Cmdline.Svc.ObjectSvc().WaitUploadingRepObjects(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading rep object: %w", err)
			}

			tb := table.NewWriter()
			if uploadObjectResult.IsUploading {

				tb.AppendHeader(table.Row{"ObjectName", "ObjectID", "FileHash"})
				for i := 0; i < len(uploadObjectResult.Objects); i++ {
					tb.AppendRow(table.Row{
						uploadObjectResult.Objects[i].ObjectName,
						uploadObjectResult.Results[i].ObjectID,
						uploadObjectResult.Results[i].FileHash,
					})
				}
				fmt.Print(tb.Render())

			} else {
				fmt.Println("The folder upload failed. Some files do not meet the upload requirements.")

				tb.AppendHeader(table.Row{"ObjectName", "Error"})
				for i := 0; i < len(uploadObjectResult.Objects); i++ {
					if uploadObjectResult.Results[i].Error != nil {
						tb.AppendRow(table.Row{uploadObjectResult.Objects[i].ObjectName, uploadObjectResult.Results[i].Error})
					}
				}
				fmt.Print(tb.Render())
			}
			return nil
		}

		if err != nil {
			return fmt.Errorf("wait uploading: %w", err)
		}
	}
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

	taskID, err := ctx.Cmdline.Svc.ObjectSvc().StartUpdatingRepObject(userID, objectID, file, fileSize)
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

	commands.MustAdd(ObjectUploadEcObject, "object", "new", "ec")

	commands.MustAdd(ObjectUploadRepObject, "object", "new", "rep")

	commands.MustAdd(ObjectUploadRepObjectDir, "object", "new", "dir")

	commands.MustAdd(ObjectDownloadObject, "object", "get")

	commands.MustAdd(ObjectDownloadObjectDir, "object", "get", "dir")

	commands.MustAdd(ObjectUpdateRepObject, "object", "update", "rep")

	commands.MustAdd(ObjectDeleteObject, "object", "delete")

}
