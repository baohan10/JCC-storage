package cmd

import (
	"gitlink.org.cn/cloudream/agent/internal/task"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	log "gitlink.org.cn/cloudream/common/pkg/logger"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
)

func (svc *Service) PinObject(msg *agtmsg.PinObject) *agtmsg.PinObjectResp {
	log.WithField("FileHash", msg.Body.FileHash).Debugf("pin object")

	// TODO 可以考虑将整个API都做成异步的
	tsk := svc.taskManager.StartCmp(task.NewIPFSPin(msg.Body.FileHash))
	tsk.Wait()

	if tsk.Error() != nil {
		log.WithField("FileHash", msg.Body.FileHash).
			Warnf("pin object failed, err: %s", tsk.Error().Error())
		return ramsg.ReplyFailed[agtmsg.PinObjectResp](errorcode.OPERATION_FAILED, "pin object failed")
	}

	return ramsg.ReplyOK(agtmsg.NewPinObjectRespBody())
}
