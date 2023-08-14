package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

type StorageService interface {
	GetStorageInfo(msg *coormsg.GetStorageInfo) (*coormsg.GetStorageInfoResp, *mq.CodeMessage)
	PreMoveObjectToStorage(msg *coormsg.PreMoveObjectToStorage) (*coormsg.PreMoveObjectToStorageResp, *mq.CodeMessage)
	MoveObjectToStorage(msg *coormsg.MoveObjectToStorage) (*coormsg.MoveObjectToStorageResp, *mq.CodeMessage)
}

func init() {
	Register(StorageService.GetStorageInfo)
	Register(StorageService.PreMoveObjectToStorage)
	Register(StorageService.MoveObjectToStorage)
}
