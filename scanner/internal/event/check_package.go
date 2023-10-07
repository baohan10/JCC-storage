package event

import (
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type CheckPackage struct {
	*scevt.CheckPackage
}

func NewCheckPackage(objIDs []int64) *CheckPackage {
	return &CheckPackage{
		CheckPackage: scevt.NewCheckPackage(objIDs),
	}
}

func (t *CheckPackage) TryMerge(other Event) bool {
	event, ok := other.(*CheckPackage)
	if !ok {
		return false
	}

	t.PackageIDs = lo.Union(t.PackageIDs, event.PackageIDs)
	return true
}

func (t *CheckPackage) Execute(execCtx ExecuteContext) {
	log := logger.WithType[CheckPackage]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	// 检查对象是否没有被引用的时候，需要读取StoragePackage表
	builder := reqbuilder.NewBuilder().Metadata().StoragePackage().ReadAny()
	for _, objID := range t.PackageIDs {
		builder.Metadata().Package().WriteOne(objID)
	}
	mutex, err := builder.MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	for _, objID := range t.PackageIDs {
		err := execCtx.Args.DB.Package().DeleteUnused(execCtx.Args.DB.SQLCtx(), objID)
		if err != nil {
			log.WithField("PackageID", objID).Warnf("delete unused package failed, err: %s", err.Error())
		}
	}
}

func init() {
	RegisterMessageConvertor(func(msg *scevt.CheckPackage) Event { return NewCheckPackage(msg.PackageIDs) })
}
