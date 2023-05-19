package task

import (
	"github.com/samber/lo"
	mysql "gitlink.org.cn/cloudream/db/sql"
	"gitlink.org.cn/cloudream/utils/logger"
)

type CheckObjectTask struct {
	ObjectIDs []int
}

func NewCheckObjectTask(objIDs []int) CheckObjectTask {
	return CheckObjectTask{
		ObjectIDs: objIDs,
	}
}

func (t *CheckObjectTask) TryMerge(other Task) bool {
	chkTask, ok := other.(*CheckObjectTask)
	if !ok {
		return false
	}

	t.ObjectIDs = lo.Union(t.ObjectIDs, chkTask.ObjectIDs)
	return true
}

func (t *CheckObjectTask) Execute(execCtx *ExecuteContext, execOpts ExecuteOption) {
	for _, objID := range t.ObjectIDs {
		err := mysql.Object.DeleteUnused(execCtx.DB.SQLCtx(), objID)
		if err != nil {
			logger.WithField("ObjectID", objID).Warnf("delete unused object failed, err: %s", err.Error())
		}
	}
}
