package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	agtmsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/agent"
)

type StorageService interface {
	StartStorageMoveObject(msg *agtmsg.StartStorageMoveObject) (*agtmsg.StartStorageMoveObjectResp, *mq.CodeMessage)

	WaitStorageMoveObject(msg *agtmsg.WaitStorageMoveObject) (*agtmsg.WaitStorageMoveObjectResp, *mq.CodeMessage)

	StorageCheck(msg *agtmsg.StorageCheck) (*agtmsg.StorageCheckResp, *mq.CodeMessage)

	StartStorageUploadRepObject(msg *agtmsg.StartStorageUploadRepObject) (*agtmsg.StartStorageUploadRepObjectResp, *mq.CodeMessage)

	WaitStorageUploadRepObject(msg *agtmsg.WaitStorageUploadRepObject) (*agtmsg.WaitStorageUploadRepObjectResp, *mq.CodeMessage)
}

func init() {
	Register(StorageService.StartStorageMoveObject)

	Register(StorageService.WaitStorageMoveObject)

	Register(StorageService.StorageCheck)

	Register(StorageService.StartStorageUploadRepObject)

	Register(StorageService.WaitStorageUploadRepObject)
}
