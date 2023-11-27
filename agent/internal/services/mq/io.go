package mq

import (
	"time"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	mytask "gitlink.org.cn/cloudream/storage/agent/internal/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

func (svc *Service) SetupIOPlan(msg *agtmq.SetupIOPlan) (*agtmq.SetupIOPlanResp, *mq.CodeMessage) {
	err := svc.sw.SetupPlan(msg.Plan)
	if err != nil {
		logger.WithField("PlanID", msg.Plan.ID).Warnf("adding plan: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "adding plan failed")
	}

	return mq.ReplyOK(agtmq.NewSetupIOPlanResp())
}

func (svc *Service) StartIOPlan(msg *agtmq.StartIOPlan) (*agtmq.StartIOPlanResp, *mq.CodeMessage) {
	tsk := svc.taskManager.StartNew(mytask.NewExecuteIOPlan(msg.PlanID))
	return mq.ReplyOK(agtmq.NewStartIOPlanResp(tsk.ID()))
}

func (svc *Service) WaitIOPlan(msg *agtmq.WaitIOPlan) (*agtmq.WaitIOPlanResp, *mq.CodeMessage) {
	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return nil, mq.Failed(errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()

		errMsg := ""
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error()
		}

		planTsk := tsk.Body().(*mytask.ExecuteIOPlan)
		return mq.ReplyOK(agtmq.NewWaitIOPlanResp(true, errMsg, planTsk.Result))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs) * time.Millisecond) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			planTsk := tsk.Body().(*mytask.ExecuteIOPlan)
			return mq.ReplyOK(agtmq.NewWaitIOPlanResp(true, errMsg, planTsk.Result))
		}

		return mq.ReplyOK(agtmq.NewWaitIOPlanResp(false, "", ioswitch.PlanResult{}))
	}
}

func (svc *Service) CancelIOPlan(msg *agtmq.CancelIOPlan) (*agtmq.CancelIOPlanResp, *mq.CodeMessage) {
	svc.sw.CancelPlan(msg.PlanID)
	return mq.ReplyOK(agtmq.NewCancelIOPlanResp())
}
