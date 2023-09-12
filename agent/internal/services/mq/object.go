package mq

import (
	"time"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

func (svc *Service) StartPinningObject(msg *agtmq.StartPinningObject) (*agtmq.StartPinningObjectResp, *mq.CodeMessage) {
	log.WithField("FileHash", msg.FileHash).Debugf("pin object")

	tsk := svc.taskManager.StartComparable(task.NewIPFSPin(msg.FileHash))

	if tsk.Error() != nil {
		log.WithField("FileHash", msg.FileHash).
			Warnf("pin object failed, err: %s", tsk.Error().Error())
		return nil, mq.Failed(errorcode.OperationFailed, "pin object failed")
	}

	return mq.ReplyOK(agtmq.NewStartPinningObjectResp(tsk.ID()))
}

func (svc *Service) WaitPinningObject(msg *agtmq.WaitPinningObject) (*agtmq.WaitPinningObjectResp, *mq.CodeMessage) {
	log.WithField("TaskID", msg.TaskID).Debugf("wait pinning object")

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

		return mq.ReplyOK(agtmq.NewWaitPinningObjectResp(true, errMsg))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs)) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			return mq.ReplyOK(agtmq.NewWaitPinningObjectResp(true, errMsg))
		}

		return mq.ReplyOK(agtmq.NewWaitPinningObjectResp(false, ""))
	}
}
