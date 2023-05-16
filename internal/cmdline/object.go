package cmdline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jedib0t/go-pretty/v6/table"
	"gitlink.org.cn/cloudream/client/internal/services"
)

func (c *Commandline) ListBucketObjects(bucketID int) error {
	userID := 0

	objects, err := services.BucketSvc(c.svc).GetBucketObjects(userID, bucketID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d objects in bucket %d for user %d:\n", len(objects), bucketID, userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "Size", "BucketID", "State", "Redundancy"})

	for _, obj := range objects {
		tb.AppendRow(table.Row{obj.ObjectID, obj.Name, obj.BucketID, obj.State, obj.FileSize, obj.Redundancy})
	}

	fmt.Print(tb.Render())
	return nil
}

func (c *Commandline) Read(localFilePath string, objectID int) error {
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
	reader, err := services.ObjectSvc(c.svc).DownloadObject(0, objectID)
	if err != nil {
		return fmt.Errorf("download object failed, err: %w", err)
	}
	defer reader.Close()

	_, err = io.Copy(outputFile, reader)
	if err != nil {
		// TODO 写入到文件失败，是否要考虑删除这个不完整的文件？
		return fmt.Errorf("copy object data to local file failed, err: %w", err)
	}

	return nil
}

func (c *Commandline) RepWrite(localFilePath string, bucketID int, objectName string, repCount int) error {
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

	err = services.ObjectSvc(c.svc).UploadRepObject(0, bucketID, objectName, file, fileSize, repCount)
	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	return nil
}

func (c *Commandline) EcWrite(localFilePath string, bucketID int, objectName string, ecName string) error {
	// TODO
	panic("not implement yet")
}

func (c *Commandline) UpdateRepObject(objectID int, filePath string) error {
	userID := 0

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

	err = services.ObjectSvc(c.svc).UpdateRepObject(userID, objectID, file, fileSize)
	if err != nil {
		return fmt.Errorf("update object %d failed, err: %w", objectID, err)
	}

	return nil
}

func (c *Commandline) DeleteObject(objectID int) error {
	userID := 0
	err := services.ObjectSvc(c.svc).DeleteObject(userID, objectID)
	if err != nil {
		return fmt.Errorf("delete object %d failed, err: %w", objectID, err)
	}
	return nil
}
