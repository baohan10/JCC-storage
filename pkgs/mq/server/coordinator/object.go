package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

type ObjectService interface {
	GetObjectsByDirName(msg *coormsg.GetObjectsByDirName) (*coormsg.GetObjectsResp, *mq.CodeMessage)
	PreDownloadObject(msg *coormsg.PreDownloadObject) (*coormsg.PreDownloadObjectResp, *mq.CodeMessage)

	PreUploadRepObject(msg *coormsg.PreUploadRepObject) (*coormsg.PreUploadResp, *mq.CodeMessage)
	CreateRepObject(msg *coormsg.CreateRepObject) (*coormsg.CreateObjectResp, *mq.CodeMessage)

	PreUpdateRepObject(msg *coormsg.PreUpdateRepObject) (*coormsg.PreUpdateRepObjectResp, *mq.CodeMessage)
	UpdateRepObject(msg *coormsg.UpdateRepObject) (*coormsg.UpdateRepObjectResp, *mq.CodeMessage)

	PreUploadEcObject(msg *coormsg.PreUploadEcObject) (*coormsg.PreUploadEcResp, *mq.CodeMessage)
	CreateEcObject(msg *coormsg.CreateEcObject) (*coormsg.CreateObjectResp, *mq.CodeMessage)

	DeleteObject(msg *coormsg.DeleteObject) (*coormsg.DeleteObjectResp, *mq.CodeMessage)
}

func init() {
	Register(ObjectService.GetObjectsByDirName)
	Register(ObjectService.PreDownloadObject)

	Register(ObjectService.PreUploadRepObject)
	Register(ObjectService.CreateRepObject)

	Register(ObjectService.PreUpdateRepObject)
	Register(ObjectService.UpdateRepObject)

	Register(ObjectService.PreUploadEcObject)
	Register(ObjectService.CreateEcObject)

	Register(ObjectService.DeleteObject)
}
