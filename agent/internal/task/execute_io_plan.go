package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

// TODO 临时使用Task来等待Plan执行进度
type ExecuteIOPlan struct {
	PlanID ioswitch.PlanID
	Result ioswitch.PlanResult
}

func NewExecuteIOPlan(planID ioswitch.PlanID) *ExecuteIOPlan {
	return &ExecuteIOPlan{
		PlanID: planID,
	}
}

func (t *ExecuteIOPlan) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	log := logger.WithType[ExecuteIOPlan]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	ret, err := ctx.sw.ExecutePlan(t.PlanID)
	if err != nil {
		err := fmt.Errorf("executing io plan: %w", err)
		log.WithField("PlanID", t.PlanID).Warn(err.Error())

		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	t.Result = ret

	complete(nil, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
