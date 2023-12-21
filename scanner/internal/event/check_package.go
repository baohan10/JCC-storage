package event

import (
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type CheckPackage struct {
	*scevt.CheckPackage
}

func NewCheckPackage(evt *scevt.CheckPackage) *CheckPackage {
	return &CheckPackage{
		CheckPackage: evt,
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
	log.Debugf("begin with %v", logger.FormatStruct(t.CheckPackage))
	defer log.Debugf("end")

	for _, objID := range t.PackageIDs {
		err := execCtx.Args.DB.Package().DeleteUnused(execCtx.Args.DB.SQLCtx(), objID)
		if err != nil {
			log.WithField("PackageID", objID).Warnf("delete unused package failed, err: %s", err.Error())
		}
	}
}

func init() {
	RegisterMessageConvertor(NewCheckPackage)
}
