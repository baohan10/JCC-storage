package services

import (
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
)

func (svc *Service) DeleteObject(msg *coormsg.DeleteObjectCommand) *coormsg.DeleteObjectResp {
	err := svc.db.SetObjectDeleted(msg.UserID, msg.ObjectID)

	if err != nil {
		return coormsg.NewDeleteObjectRespFailed(errorcode.OPERATION_FAILED, "set object deleted failed")
	}

	return coormsg.NewDeleteObjectRespOK()
}
