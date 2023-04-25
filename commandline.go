package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jedib0t/go-pretty/v6/table"
	"gitlink.org.cn/cloudream/client/config"
	"gitlink.org.cn/cloudream/client/services"
	myio "gitlink.org.cn/cloudream/utils/io"
)

func DispatchCommand(cmd string, args []string) {
	switch cmd {
	case "read":
		objectID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid object id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}

		if err := Read(args[0], objectID); err != nil {
			fmt.Printf("read failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "write":
		bucketID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}
		numRep, _ := strconv.Atoi(args[3])
		if numRep <= 0 || numRep > config.Cfg().MaxReplicateNumber {
			fmt.Printf("replicate number should not be more than %d", config.Cfg().MaxReplicateNumber)
			os.Exit(1)
		}

		if err := RepWrite(args[0], bucketID, args[2], numRep); err != nil {
			fmt.Printf("rep write failed, err: %s", err.Error())
			os.Exit(1)
		}
	case "ecWrite":
		bucketID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}
		if err := EcWrite(args[0], bucketID, args[2], args[3]); err != nil {
			fmt.Printf("ec write failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "move":
		objectID, err := strconv.Atoi(args[0])
		if err != nil {
			fmt.Printf("invalid object id %s, err: %s", args[0], err.Error())
			os.Exit(1)
		}
		stgID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid storage id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}

		if err := Move(objectID, stgID); err != nil {
			fmt.Printf("move failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "ls":
		if len(args) == 0 {
			if err := GetUserBuckets(); err != nil {
				fmt.Printf("get user buckets failed, err: %s", err.Error())
				os.Exit(1)
			}
		} else {
			bucketID, err := strconv.Atoi(args[0])
			if err != nil {
				fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
				os.Exit(1)
			}

			if err := GetBucketObjects(bucketID); err != nil {
				fmt.Printf("get bucket objects failed, err: %s", err.Error())
				os.Exit(1)
			}
		}
	}
}

func Read(localFilePath string, objectID int) error {
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
	reader, err := services.ObjectSvc(svc).DownloadObject(0, objectID)
	if err != nil {
		return fmt.Errorf("download object failed, err: %w", err)
	}
	defer reader.Close()

	buf := make([]byte, 1024)
	for {
		readCnt, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return fmt.Errorf("read object data failed, err: %w", err)
		}

		err = myio.WriteAll(outputFile, buf[:readCnt])
		// TODO 写入到文件失败，是否要考虑删除这个不完整的文件？
		if err != nil {
			return fmt.Errorf("write object data to local file failed, err: %w", err)
		}
	}
}

func RepWrite(localFilePath string, bucketID int, objectName string, repNum int) error {
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

	err = services.ObjectSvc(svc).UploadRepObject(0, bucketID, objectName, file, fileSize, repNum)
	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	return nil
}

func Move(objectID int, storageID int) error {
	return services.StorageSvc(svc).MoveObjectToStorage(0, objectID, storageID)
}

func EcWrite(localFilePath string, bucketID int, objectName string, ecName string) error {
	// TODO
	panic("not implement yet")
}

func GetUserBuckets() error {
	userID := 0

	buckets, err := services.BucketSvc(svc).GetUserBuckets(userID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d buckets for user %d:\n", len(buckets), userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "CreatorID"})

	for _, bucket := range buckets {
		tb.AppendRow(table.Row{bucket.BucketID, bucket.Name, bucket.CreatorID})
	}

	fmt.Print(tb.Render())
	return nil
}

func GetBucketObjects(bucketID int) error {
	userID := 0

	objects, err := services.BucketSvc(svc).GetBucketObjects(userID, bucketID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d objects in bucket %d for user %d:\n", len(objects), bucketID, userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "Size", "Redundancy", "NumRep", "ECName"})

	for _, obj := range objects {
		tb.AppendRow(table.Row{obj.ObjectID, obj.Name, obj.FileSizeInBytes, obj.Redundancy, obj.NumRep, obj.ECName})
	}

	fmt.Print(tb.Render())
	return nil
}

func CreateBucket(bucketName string) error {
	userID := 0

	bucketID, err := services.BucketSvc(svc).CreateBucket(userID, bucketName)
	if err != nil {
		return err
	}

	fmt.Printf("Create bucket %s success, id: %d", bucketName, bucketID)
	return nil
}
