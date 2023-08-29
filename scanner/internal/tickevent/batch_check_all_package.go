package tickevent

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage-scanner/internal/event"
)

const CheckPackageBatchSize = 100

type BatchCheckAllPackage struct {
	lastCheckStart int
}

func NewBatchCheckAllPackage() *BatchCheckAllPackage {
	return &BatchCheckAllPackage{}
}

func (e *BatchCheckAllPackage) Execute(ctx ExecuteContext) {
	log := logger.WithType[BatchCheckAllPackage]("TickEvent")
	log.Debugf("begin")
	defer log.Debugf("end")

	packageIDs, err := ctx.Args.DB.Package().BatchGetAllPackageIDs(ctx.Args.DB.SQLCtx(), e.lastCheckStart, CheckPackageBatchSize)
	if err != nil {
		log.Warnf("batch get package ids failed, err: %s", err.Error())
		return
	}

	ctx.Args.EventExecutor.Post(event.NewCheckPackage(packageIDs))

	// 如果结果的长度小于预期的长度，则认为已经查询了所有，下次从头再来
	if len(packageIDs) < CheckPackageBatchSize {
		e.lastCheckStart = 0
		log.Debugf("all package checked, next time will start check at 0")

	} else {
		e.lastCheckStart += CheckPackageBatchSize
	}
}
