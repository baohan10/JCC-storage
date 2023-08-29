package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage-scanner/internal/event"
)

const CHECK_CACHE_BATCH_SIZE = 100

type BatchCheckAllRepCount struct {
	lastCheckStart int
}

func NewBatchCheckAllRepCount() *BatchCheckAllRepCount {
	return &BatchCheckAllRepCount{}
}

func (e *BatchCheckAllRepCount) Execute(ctx ExecuteContext) {
	log := logger.WithType[BatchCheckAllRepCount]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	fileHashes, err := ctx.Args.DB.Cache().BatchGetAllFileHashes(ctx.Args.DB.SQLCtx(), e.lastCheckStart, CHECK_CACHE_BATCH_SIZE)
	if err != nil {
		log.Warnf("batch get file hashes failed, err: %s", err.Error())
		return
	}

	ctx.Args.EventExecutor.Post(event.NewCheckRepCount(fileHashes))

	// 如果结果的长度小于预期的长度，则认为已经查询了所有，下次从头再来
	if len(fileHashes) < CHECK_CACHE_BATCH_SIZE {
		e.lastCheckStart = 0
		log.Debugf("all rep count checked, next time will start check at 0")

	} else {
		e.lastCheckStart += CHECK_CACHE_BATCH_SIZE
	}
}
