package mq

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/agent/internal/task"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

func (svc *Service) PinObject(msg *agtmq.PinObject) (*agtmq.PinObjectResp, *mq.CodeMessage) {
	logger.WithField("FileHash", msg.FileHashes).Debugf("pin object")

	tsk := svc.taskManager.StartNew(task.NewIPFSPin(msg.FileHashes))

	if tsk.Error() != nil {
		logger.WithField("FileHash", msg.FileHashes).
			Warnf("pin object failed, err: %s", tsk.Error().Error())
		return nil, mq.Failed(errorcode.OperationFailed, "pin object failed")
	}

	if msg.IsBackground {
		return mq.ReplyOK(agtmq.RespPinObject())
	}

	tsk.Wait()
	return mq.ReplyOK(agtmq.RespPinObject())
}
