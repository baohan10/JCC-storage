package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
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
	defer log.Debugf("end")

	storageIDs, err := ctx.Args.DB.Storage().BatchGetAllStorageIDs(ctx.Args.DB.SQLCtx(), e.lastCheckStart, CHECK_STORAGE_BATCH_SIZE)
	if err != nil {
		log.Warnf("batch get storage ids failed, err: %s", err.Error())
		return
	}

	for _, stgID := range storageIDs {
		// 设置nil代表进行全量检查
		ctx.Args.EventExecutor.Post(event.NewAgentCheckStorage(scevt.NewAgentCheckStorage(stgID)))
	}

	// 如果结果的长度小于预期的长度，则认为已经查询了所有，下次从头再来
	if len(storageIDs) < CHECK_STORAGE_BATCH_SIZE {
		e.lastCheckStart = 0
		log.Debugf("all storage checked, next time will start check at 0")

	} else {
		e.lastCheckStart += CHECK_STORAGE_BATCH_SIZE
	}
}
