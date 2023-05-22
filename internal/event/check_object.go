package event

import (
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/utils/logger"
	mysql "gitlink.org.cn/cloudream/db/sql"
)

type CheckObject struct {
	ObjectIDs []int
}

func NewCheckObject(objIDs []int) CheckObject {
	return CheckObject{
		ObjectIDs: objIDs,
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
	for _, objID := range t.ObjectIDs {
		err := mysql.Object.DeleteUnused(execCtx.Args.DB.SQLCtx(), objID)
		if err != nil {
			logger.WithField("ObjectID", objID).Warnf("delete unused object failed, err: %s", err.Error())
		}
	}
}
