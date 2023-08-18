package task

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
)

type IPFSRead struct {
	FileHash  string
	LocalPath string
}

func NewIPFSRead(fileHash string, localPath string) *IPFSRead {
	return &IPFSRead{
		FileHash:  fileHash,
		LocalPath: localPath,
	}
}

func (t *IPFSRead) Compare(other *Task) bool {
	tsk, ok := other.Body().(*IPFSRead)
	if !ok {
		return false
	}

	return t.FileHash == tsk.FileHash && t.LocalPath == tsk.LocalPath
}

func (t *IPFSRead) Execute(ctx TaskContext, complete CompleteFn) {
	log := logger.WithType[IPFSRead]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	outputFileDir := filepath.Dir(t.LocalPath)

	err := os.MkdirAll(outputFileDir, os.ModePerm)
	if err != nil {
		err := fmt.Errorf("create output file directory %s failed, err: %w", outputFileDir, err)
		log.WithField("LocalPath", t.LocalPath).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	outputFile, err := os.Create(t.LocalPath)
	if err != nil {
		err := fmt.Errorf("create output file %s failed, err: %w", t.LocalPath, err)
		log.WithField("LocalPath", t.LocalPath).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}
	defer outputFile.Close()

	rd, err := ctx.ipfs.OpenRead(t.FileHash)
	if err != nil {
		err := fmt.Errorf("read ipfs file failed, err: %w", err)
		log.WithField("FileHash", t.FileHash).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	_, err = io.Copy(outputFile, rd)
	if err != nil {
		err := fmt.Errorf("copy ipfs file to local file failed, err: %w", err)
		log.WithField("LocalPath", t.LocalPath).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	complete(nil, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
