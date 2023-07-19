package cmd

import (
	"time"

	"gitlink.org.cn/cloudream/agent/internal/task"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
)

func (svc *Service) StartPinningObject(msg *agtmsg.StartPinningObject) *agtmsg.StartPinningObjectResp {
	log.WithField("FileHash", msg.Body.FileHash).Debugf("pin object")

	tsk := svc.taskManager.StartComparable(task.NewIPFSPin(msg.Body.FileHash))

	if tsk.Error() != nil {
		log.WithField("FileHash", msg.Body.FileHash).
			Warnf("pin object failed, err: %s", tsk.Error().Error())
		return ramsg.ReplyFailed[agtmsg.StartPinningObjectResp](errorcode.OPERATION_FAILED, "pin object failed")
	}

	return ramsg.ReplyOK(agtmsg.NewStartPinningObjectRespBody(tsk.ID()))
}

func (svc *Service) WaitPinningObject(msg *agtmsg.WaitPinningObject) *agtmsg.WaitPinningObjectResp {
	log.WithField("TaskID", msg.Body.TaskID).Debugf("wait pinning object")

	tsk := svc.taskManager.FindByID(msg.Body.TaskID)
	if tsk == nil {
		return ramsg.ReplyFailed[agtmsg.WaitPinningObjectResp](errorcode.TASK_NOT_FOUND, "task not found")
	}

	if msg.Body.WaitTimeoutMs == 0 {
		tsk.Wait()

		errMsg := ""
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error()
		}

		return ramsg.ReplyOK(agtmsg.NewWaitPinningObjectRespBody(true, errMsg))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.Body.WaitTimeoutMs)) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			return ramsg.ReplyOK(agtmsg.NewWaitPinningObjectRespBody(true, errMsg))
		}

		return ramsg.ReplyOK(agtmsg.NewWaitPinningObjectRespBody(false, ""))
	}
}
