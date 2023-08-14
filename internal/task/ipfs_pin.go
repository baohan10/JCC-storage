package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkg/logger"
)

type IPFSPin struct {
	FileHash string
}

func NewIPFSPin(fileHash string) *IPFSPin {
	return &IPFSPin{
		FileHash: fileHash,
	}
}

func (t *IPFSPin) Compare(other *Task) bool {
	tsk, ok := other.Body().(*IPFSPin)
	if !ok {
		return false
	}

	return t.FileHash == tsk.FileHash
}

func (t *IPFSPin) Execute(ctx TaskContext, complete CompleteFn) {
	log := logger.WithType[IPFSPin]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	err := ctx.IPFS.Pin(t.FileHash)
	if err != nil {
		err := fmt.Errorf("pin file failed, err: %w", err)
		log.WithField("FileHash", t.FileHash).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	complete(nil, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
