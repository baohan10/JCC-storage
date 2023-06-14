package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

const CHECK_STORAGE_BATCH_SIZE = 5

type BatchCheckAllStorage struct {
	lastCheckStart int
}

func NewBatchCheckAllStorage() *BatchCheckAllStorage {
	return &BatchCheckAllStorage{}
}

func (e *BatchCheckAllStorage) Execute(ctx ExecuteContext) {
	log := logger.WithType[BatchCheckAllStorage]("TickEvent")
	log.Debugf("begin")

	storageIDs, err := ctx.Args.DB.Storage().BatchGetAllStorageIDs(ctx.Args.DB.SQLCtx(), e.lastCheckStart, CHECK_STORAGE_BATCH_SIZE)
	if err != nil {
		log.Warnf("batch get storage ids failed, err: %s", err.Error())
		return
	}

	for _, stgID := range storageIDs {
		// 设置nil代表进行全量检查
		// TODO 将ID字段换成int64类型
		ctx.Args.EventExecutor.Post(event.NewAgentCheckStorage(int(stgID), nil))
	}

	// 如果结果的长度小于预期的长度，则认为已经查询了所有，下次从头再来
	if len(storageIDs) < CHECK_STORAGE_BATCH_SIZE {
		e.lastCheckStart = 0
		log.Debugf("all storage checked, next time will start check at 0")

	} else {
		e.lastCheckStart += CHECK_STORAGE_BATCH_SIZE
	}
}
