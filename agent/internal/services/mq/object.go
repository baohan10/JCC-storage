package mq

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

func (svc *Service) PinObject(msg *agtmq.PinObject) (*agtmq.PinObjectResp, *mq.CodeMessage) {
	log.WithField("FileHash", msg.FileHash).Debugf("pin object")

	tsk := svc.taskManager.StartComparable(task.NewIPFSPin(msg.FileHash))

	if tsk.Error() != nil {
		log.WithField("FileHash", msg.FileHash).
			Warnf("pin object failed, err: %s", tsk.Error().Error())
		return nil, mq.Failed(errorcode.OperationFailed, "pin object failed")
	}

	if msg.IsBackground {
		return mq.ReplyOK(agtmq.RespPinObject())
	}

	tsk.Wait()
	return mq.ReplyOK(agtmq.RespPinObject())
}
