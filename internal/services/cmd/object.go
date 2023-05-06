package cmd

import (
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
	log "gitlink.org.cn/cloudream/utils/logger"
)

func (svc *Service) PinObject(msg *agtmsg.PinObject) *agtmsg.PinObjectResp {
	log.WithField("FileHash", msg.Body.FileHash).Debugf("pin object")

	err := svc.ipfs.Pin(msg.Body.FileHash)
	if err != nil {
		log.WithField("FileHash", msg.Body.FileHash).
			Warnf("pin object failed, err: %s", err.Error())
		return ramsg.ReplyFailed[agtmsg.PinObjectResp](errorcode.OPERATION_FAILED, "pin object failed")
	}

	return ramsg.ReplyOK(agtmsg.NewPinObjectRespBody())
}
