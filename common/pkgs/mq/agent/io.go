package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type IOService interface {
	SetupIOPlan(msg *SetupIOPlan) (*SetupIOPlanResp, *mq.CodeMessage)

	StartIOPlan(msg *StartIOPlan) (*StartIOPlanResp, *mq.CodeMessage)

	WaitIOPlan(msg *WaitIOPlan) (*WaitIOPlanResp, *mq.CodeMessage)

	CancelIOPlan(msg *CancelIOPlan) (*CancelIOPlanResp, *mq.CodeMessage)
}

// 设置io计划
var _ = Register(Service.SetupIOPlan)

type SetupIOPlan struct {
	mq.MessageBodyBase
	Plan ioswitch.Plan `json:"plan"`
}
type SetupIOPlanResp struct {
	mq.MessageBodyBase
}

func NewSetupIOPlan(plan ioswitch.Plan) *SetupIOPlan {
	return &SetupIOPlan{
		Plan: plan,
	}
}
func NewSetupIOPlanResp() *SetupIOPlanResp {
	return &SetupIOPlanResp{}
}
func (client *Client) SetupIOPlan(msg *SetupIOPlan, opts ...mq.RequestOption) (*SetupIOPlanResp, error) {
	return mq.Request(Service.SetupIOPlan, client.rabbitCli, msg, opts...)
}

// 启动io计划
var _ = Register(Service.StartIOPlan)

type StartIOPlan struct {
	mq.MessageBodyBase
	PlanID ioswitch.PlanID `json:"planID"`
}
type StartIOPlanResp struct {
	mq.MessageBodyBase
	TaskID string `json:"taskID"`
}

func NewStartIOPlan(planID ioswitch.PlanID) *StartIOPlan {
	return &StartIOPlan{
		PlanID: planID,
	}
}
func NewStartIOPlanResp(taskID string) *StartIOPlanResp {
	return &StartIOPlanResp{
		TaskID: taskID,
	}
}
func (client *Client) StartIOPlan(msg *StartIOPlan, opts ...mq.RequestOption) (*StartIOPlanResp, error) {
	return mq.Request(Service.StartIOPlan, client.rabbitCli, msg, opts...)
}

// 启动io计划
var _ = Register(Service.WaitIOPlan)

type WaitIOPlan struct {
	mq.MessageBodyBase
	TaskID        string `json:"taskID"`
	WaitTimeoutMs int64  `json:"waitTimeout"`
}
type WaitIOPlanResp struct {
	mq.MessageBodyBase
	IsComplete bool                `json:"isComplete"`
	Error      string              `json:"error"`
	Result     ioswitch.PlanResult `json:"result"`
}

func NewWaitIOPlan(taskID string, waitTimeoutMs int64) *WaitIOPlan {
	return &WaitIOPlan{
		TaskID:        taskID,
		WaitTimeoutMs: waitTimeoutMs,
	}
}
func NewWaitIOPlanResp(isComplete bool, err string, result ioswitch.PlanResult) *WaitIOPlanResp {
	return &WaitIOPlanResp{
		IsComplete: isComplete,
		Error:      err,
		Result:     result,
	}
}
func (client *Client) WaitIOPlan(msg *WaitIOPlan, opts ...mq.RequestOption) (*WaitIOPlanResp, error) {
	return mq.Request(Service.WaitIOPlan, client.rabbitCli, msg, opts...)
}

// 取消io计划
var _ = Register(Service.CancelIOPlan)

type CancelIOPlan struct {
	mq.MessageBodyBase
	PlanID ioswitch.PlanID `json:"planID"`
}
type CancelIOPlanResp struct {
	mq.MessageBodyBase
}

func NewCancelIOPlan(planID ioswitch.PlanID) *CancelIOPlan {
	return &CancelIOPlan{
		PlanID: planID,
	}
}
func NewCancelIOPlanResp() *CancelIOPlanResp {
	return &CancelIOPlanResp{}
}
func (client *Client) CancelIOPlan(msg *CancelIOPlan, opts ...mq.RequestOption) (*CancelIOPlanResp, error) {
	return mq.Request(Service.CancelIOPlan, client.rabbitCli, msg, opts...)
}
