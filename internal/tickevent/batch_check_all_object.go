package tickevent

import (
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

const CHECK_OBJECT_BATCH_SIZE = 100

type BatchCheckAllObject struct {
	lastCheckStart int
}

func NewBatchCheckAllObject() *BatchCheckAllObject {
	return &BatchCheckAllObject{}
}

func (e *BatchCheckAllObject) Execute(ctx ExecuteContext) {
	log := logger.WithType[BatchCheckAllObject]("TickEvent")
	log.Debugf("begin")

	objectIDs, err := mysql.Object.BatchGetAllObjectIDs(ctx.Args.DB.SQLCtx(), e.lastCheckStart, CHECK_OBJECT_BATCH_SIZE)
	if err != nil {
		log.Warnf("batch get object ids failed, err: %s", err.Error())
		return
	}

	// TODO 将ID字段换成int64类型
	ctx.Args.EventExecutor.Post(event.NewCheckObject(lo.Map(objectIDs, func(id int64, index int) int { return int(id) })))

	// 如果结果的长度小于预期的长度，则认为已经查询了所有，下次从头再来
	if len(objectIDs) < CHECK_OBJECT_BATCH_SIZE {
		e.lastCheckStart = 0
		log.Debugf("all object checked, next time will start check at 0")

	} else {
		e.lastCheckStart += CHECK_OBJECT_BATCH_SIZE
	}
}
