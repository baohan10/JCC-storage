package cmd

import (
	"time"

	"gitlink.org.cn/cloudream/agent/internal/task"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
)

func (svc *Service) StartPinningObject(msg *agtmsg.StartPinningObject) (*agtmsg.StartPinningObjectResp, *ramsg.CodeMessage) {
	log.WithField("FileHash", msg.FileHash).Debugf("pin object")

	tsk := svc.taskManager.StartComparable(task.NewIPFSPin(msg.FileHash))

	if tsk.Error() != nil {
		log.WithField("FileHash", msg.FileHash).
			Warnf("pin object failed, err: %s", tsk.Error().Error())
		return ramsg.ReplyFailed[agtmsg.StartPinningObjectResp](errorcode.OPERATION_FAILED, "pin object failed")
	}

	return ramsg.ReplyOK(agtmsg.NewStartPinningObjectResp(tsk.ID()))
}

func (svc *Service) WaitPinningObject(msg *agtmsg.WaitPinningObject) (*agtmsg.WaitPinningObjectResp, *ramsg.CodeMessage) {
	log.WithField("TaskID", msg.TaskID).Debugf("wait pinning object")

	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return ramsg.ReplyFailed[agtmsg.WaitPinningObjectResp](errorcode.TASK_NOT_FOUND, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()

		errMsg := ""
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error()
		}

		return ramsg.ReplyOK(agtmsg.NewWaitPinningObjectResp(true, errMsg))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs)) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			return ramsg.ReplyOK(agtmsg.NewWaitPinningObjectResp(true, errMsg))
		}

		return ramsg.ReplyOK(agtmsg.NewWaitPinningObjectResp(false, ""))
	}
}
