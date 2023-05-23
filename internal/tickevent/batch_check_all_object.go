package tickevent

import (
	"gitlink.org.cn/cloudream/common/utils/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

const CHECK_OBJECT_BATCH_SIZE = 100

type BatchCheckObject struct {
	lastCheckStart int
}

func (e *BatchCheckObject) Execute(ctx ExecuteContext) {
	objectIDs, err := mysql.Object.BatchGetAllObjectIDs(ctx.Args.DB.SQLCtx(), e.lastCheckStart, CHECK_OBJECT_BATCH_SIZE)
	if err != nil {
		logger.Warnf("batch get object ids failed, err: %s", err.Error())
		return
	}

	ctx.Args.EventExecutor.Post(event.NewCheckObject(objectIDs))

	// 如果结果的长度小于预期的长度，则认为已经查询了所有，下次从头再来
	if len(objectIDs) < CHECK_OBJECT_BATCH_SIZE {
		e.lastCheckStart = 0
		logger.Debugf("all object checked, next time will start check at 0")

	} else {
		e.lastCheckStart += CHECK_OBJECT_BATCH_SIZE
	}
}
