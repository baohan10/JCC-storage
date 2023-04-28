package cmdline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jedib0t/go-pretty/v6/table"
	"gitlink.org.cn/cloudream/client/internal/services"
	myio "gitlink.org.cn/cloudream/utils/io"
)

func (c *Commandline) GetBucketObjects(bucketID int) error {
	userID := 0

	objects, err := services.BucketSvc(c.svc).GetBucketObjects(userID, bucketID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d objects in bucket %d for user %d:\n", len(objects), bucketID, userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "Size", "BucketID", "State", "Redundancy", "NumRep", "ECName"})

	for _, obj := range objects {
		tb.AppendRow(table.Row{obj.ObjectID, obj.Name, obj.BucketID, obj.State, obj.FileSizeInBytes, obj.Redundancy, obj.NumRep, obj.ECName})
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

	/*
		TO DO: 判断本地有没有ipfs daemon、能否获取相应对象的cid
			如果本地有ipfs daemon且能获取相应对象的cid，则获取对象cid对应的ipfsblock的cid，通过ipfs网络获取这些ipfsblock
			否则，像目前一样，使用grpc向指定节点获取
	*/
	// 下载文件
	reader, err := services.ObjectSvc(c.svc).DownloadObject(0, objectID)
	if err != nil {
		return fmt.Errorf("download object failed, err: %w", err)
	}
	defer reader.Close()

	buf := make([]byte, 1024)
	for {
		readCnt, err := reader.Read(buf)

		if readCnt > 0 {
			err = myio.WriteAll(outputFile, buf[:readCnt])
			// TODO 写入到文件失败，是否要考虑删除这个不完整的文件？
			if err != nil {
				return fmt.Errorf("write object data to local file failed, err: %w", err)
			}
		}

		if err != nil {
			if err == io.EOF {
				return nil
			}

			return fmt.Errorf("read object data failed, err: %w", err)
		}
	}
}

func (c *Commandline) RepWrite(localFilePath string, bucketID int, objectName string, repNum int) error {
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

	err = services.ObjectSvc(c.svc).UploadRepObject(0, bucketID, objectName, file, fileSize, repNum)
	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	return nil
}

func (c *Commandline) EcWrite(localFilePath string, bucketID int, objectName string, ecName string) error {
	// TODO
	panic("not implement yet")
}

func (c *Commandline) DeleteObject(objectID int) error {
	userID := 0
	err := services.ObjectSvc(c.svc).DeleteObject(userID, objectID)
	if err != nil {
		return fmt.Errorf("delete object %d failed, err: %w", objectID, err)
	}
	return nil
}
