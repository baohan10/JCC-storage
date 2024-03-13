package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
)

type IPFSPin struct {
	FileHashes []string
}

func NewIPFSPin(fileHashes []string) *IPFSPin {
	return &IPFSPin{
		FileHashes: fileHashes,
	}
}

func (t *IPFSPin) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	log := logger.WithType[IPFSPin]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		err := fmt.Errorf("new ipfs client: %w", err)
		log.Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}
	defer ipfsCli.Close()

	for _, fileHash := range t.FileHashes {
		err = ipfsCli.Pin(fileHash)
		if err != nil {
			err := fmt.Errorf("pin file failed, err: %w", err)
			log.WithField("FileHash", t.FileHashes).Warn(err.Error())

			complete(err, CompleteOption{
				RemovingDelay: time.Minute,
			})
			return
		}
	}

	complete(nil, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
