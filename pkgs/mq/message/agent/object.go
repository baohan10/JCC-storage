package agent

import "gitlink.org.cn/cloudream/common/pkg/mq"

type StartPinningObject struct {
	FileHash string `json:"fileHash"`
}

func NewStartPinningObject(fileHash string) StartPinningObject {
	return StartPinningObject{
		FileHash: fileHash,
	}
}

type StartPinningObjectResp struct {
	TaskID string `json:"taskID"`
}

func NewStartPinningObjectResp(taskID string) StartPinningObjectResp {
	return StartPinningObjectResp{
		TaskID: taskID,
	}
}

type WaitPinningObject struct {
	TaskID        string `json:"taskID"`
	WaitTimeoutMs int64  `json:"waitTimeout"`
}

func NewWaitPinningObject(taskID string, waitTimeoutMs int64) WaitPinningObject {
	return WaitPinningObject{
		TaskID:        taskID,
		WaitTimeoutMs: waitTimeoutMs,
	}
}

type WaitPinningObjectResp struct {
	IsComplete bool   `json:"isComplete"`
	Error      string `json:"error"`
}

func NewWaitPinningObjectResp(isComplete bool, err string) WaitPinningObjectResp {
	return WaitPinningObjectResp{
		IsComplete: isComplete,
		Error:      err,
	}
}

func init() {
	mq.RegisterMessage[StartPinningObject]()
	mq.RegisterMessage[StartPinningObjectResp]()

	mq.RegisterMessage[WaitPinningObject]()
	mq.RegisterMessage[WaitPinningObjectResp]()
}
