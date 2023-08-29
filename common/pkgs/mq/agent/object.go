package agent

import "gitlink.org.cn/cloudream/common/pkgs/mq"

type ObjectService interface {
	StartPinningObject(msg *StartPinningObject) (*StartPinningObjectResp, *mq.CodeMessage)
	WaitPinningObject(msg *WaitPinningObject) (*WaitPinningObjectResp, *mq.CodeMessage)
}

// 启动Pin对象的任务
var _ = Register(ObjectService.StartPinningObject)

type StartPinningObject struct {
	FileHash string `json:"fileHash"`
}
type StartPinningObjectResp struct {
	TaskID string `json:"taskID"`
}

func NewStartPinningObject(fileHash string) StartPinningObject {
	return StartPinningObject{
		FileHash: fileHash,
	}
}
func NewStartPinningObjectResp(taskID string) StartPinningObjectResp {
	return StartPinningObjectResp{
		TaskID: taskID,
	}
}
func (client *Client) StartPinningObject(msg StartPinningObject, opts ...mq.RequestOption) (*StartPinningObjectResp, error) {
	return mq.Request[StartPinningObjectResp](client.rabbitCli, msg, opts...)
}

// 等待Pin对象的任务
var _ = Register(ObjectService.WaitPinningObject)

type WaitPinningObject struct {
	TaskID        string `json:"taskID"`
	WaitTimeoutMs int64  `json:"waitTimeout"`
}
type WaitPinningObjectResp struct {
	IsComplete bool   `json:"isComplete"`
	Error      string `json:"error"`
}

func NewWaitPinningObject(taskID string, waitTimeoutMs int64) WaitPinningObject {
	return WaitPinningObject{
		TaskID:        taskID,
		WaitTimeoutMs: waitTimeoutMs,
	}
}
func NewWaitPinningObjectResp(isComplete bool, err string) WaitPinningObjectResp {
	return WaitPinningObjectResp{
		IsComplete: isComplete,
		Error:      err,
	}
}
func (client *Client) WaitPinningObject(msg WaitPinningObject, opts ...mq.RequestOption) (*WaitPinningObjectResp, error) {
	return mq.Request[WaitPinningObjectResp](client.rabbitCli, msg, opts...)
}
