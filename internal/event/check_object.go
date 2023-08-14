package event

import (
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	scevt "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/scanner/event"
)

type CheckObject struct {
	scevt.CheckObject
}

func NewCheckObject(objIDs []int64) *CheckObject {
	return &CheckObject{
		CheckObject: scevt.NewCheckObject(objIDs),
	}
}

func (t *CheckObject) TryMerge(other Event) bool {
	event, ok := other.(*CheckObject)
	if !ok {
		return false
	}

	t.ObjectIDs = lo.Union(t.ObjectIDs, event.ObjectIDs)
	return true
}

func (t *CheckObject) Execute(execCtx ExecuteContext) {
	log := logger.WithType[CheckObject]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	// 检查对象是否没有被引用的时候，需要读取StorageObject表
	builder := reqbuilder.NewBuilder().Metadata().StorageObject().ReadAny()
	for _, objID := range t.ObjectIDs {
		builder.Metadata().Object().WriteOne(objID)
	}
	mutex, err := builder.MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquire locks failed, err: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	for _, objID := range t.ObjectIDs {
		err := execCtx.Args.DB.Object().DeleteUnused(execCtx.Args.DB.SQLCtx(), objID)
		if err != nil {
			log.WithField("ObjectID", objID).Warnf("delete unused object failed, err: %s", err.Error())
		}
	}
}

func init() {
	RegisterMessageConvertor(func(msg scevt.CheckObject) Event { return NewCheckObject(msg.ObjectIDs) })
}
