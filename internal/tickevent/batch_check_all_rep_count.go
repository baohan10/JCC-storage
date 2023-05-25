package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkg/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

const CHECK_CACHE_BATCH_SIZE = 100

type BatchCheckAllRepCount struct {
	lastCheckStart int
}

func NewBatchCheckAllRepCount() *BatchCheckAllRepCount {
	return &BatchCheckAllRepCount{}
}

func (e *BatchCheckAllRepCount) Execute(ctx ExecuteContext) {
	fileHashes, err := mysql.Cache.BatchGetAllFileHashes(ctx.Args.DB.SQLCtx(), e.lastCheckStart, CHECK_CACHE_BATCH_SIZE)
	if err != nil {
		logger.Warnf("batch get file hashes failed, err: %s", err.Error())
		return
	}

	ctx.Args.EventExecutor.Post(event.NewCheckRepCount(fileHashes))

	// 如果结果的长度小于预期的长度，则认为已经查询了所有，下次从头再来
	if len(fileHashes) < CHECK_CACHE_BATCH_SIZE {
		e.lastCheckStart = 0
		logger.Debugf("all rep count checked, next time will start check at 0")

	} else {
		e.lastCheckStart += CHECK_CACHE_BATCH_SIZE
	}
}
