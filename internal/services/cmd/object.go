package cmd

import (
	"time"

	"gitlink.org.cn/cloudream/common/consts/errorcode"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/common/pkg/mq"
	"gitlink.org.cn/cloudream/storage-agent/internal/task"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

func (svc *Service) StartPinningObject(msg *agtmsg.StartPinningObject) (*agtmsg.StartPinningObjectResp, *mq.CodeMessage) {
	log.WithField("FileHash", msg.FileHash).Debugf("pin object")

	tsk := svc.taskManager.StartComparable(task.NewIPFSPin(msg.FileHash))

	if tsk.Error() != nil {
		log.WithField("FileHash", msg.FileHash).
			Warnf("pin object failed, err: %s", tsk.Error().Error())
		return mq.ReplyFailed[agtmsg.StartPinningObjectResp](errorcode.OperationFailed, "pin object failed")
	}

	return mq.ReplyOK(agtmsg.NewStartPinningObjectResp(tsk.ID()))
}

func (svc *Service) WaitPinningObject(msg *agtmsg.WaitPinningObject) (*agtmsg.WaitPinningObjectResp, *mq.CodeMessage) {
	log.WithField("TaskID", msg.TaskID).Debugf("wait pinning object")

	tsk := svc.taskManager.FindByID(msg.TaskID)
	if tsk == nil {
		return mq.ReplyFailed[agtmsg.WaitPinningObjectResp](errorcode.TaskNotFound, "task not found")
	}

	if msg.WaitTimeoutMs == 0 {
		tsk.Wait()

		errMsg := ""
		if tsk.Error() != nil {
			errMsg = tsk.Error().Error()
		}

		return mq.ReplyOK(agtmsg.NewWaitPinningObjectResp(true, errMsg))

	} else {
		if tsk.WaitTimeout(time.Duration(msg.WaitTimeoutMs)) {

			errMsg := ""
			if tsk.Error() != nil {
				errMsg = tsk.Error().Error()
			}

			return mq.ReplyOK(agtmsg.NewWaitPinningObjectResp(true, errMsg))
		}

		return mq.ReplyOK(agtmsg.NewWaitPinningObjectResp(false, ""))
	}
}
