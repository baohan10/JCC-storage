package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/models"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type GetStorageInfo struct {
	UserID    int64 `json:"userID"`
	StorageID int64 `json:"storageID"`
}

func NewGetStorageInfo(userID int64, storageID int64) GetStorageInfo {
	return GetStorageInfo{
		UserID:    userID,
		StorageID: storageID,
	}
}

type GetStorageInfoResp struct {
	model.Storage
}

func NewGetStorageInfoResp(storageID int64, name string, nodeID int64, dir string, state string) GetStorageInfoResp {
	return GetStorageInfoResp{
		model.Storage{
			StorageID: storageID,
			Name:      name,
			NodeID:    nodeID,
			Directory: dir,
			State:     state,
		},
	}
}

// 客户端发给协调端，告知要调度数据
type PreMoveObjectToStorage struct {
	ObjectID  int64 `json:"objectID"`
	StorageID int64 `json:"storageID"`
	UserID    int64 `json:"userID"`
}

func NewPreMoveObjectToStorage(objectID int64, stgID int64, userID int64) PreMoveObjectToStorage {
	return PreMoveObjectToStorage{
		ObjectID:  objectID,
		StorageID: stgID,
		UserID:    userID,
	}
}

// 协调端发给客户端，告知要调度数据的详情
type PreMoveObjectToStorageResp struct {
	NodeID     int64                      `json:"nodeID"`
	Directory  string                     `json:"directory"`
	Object     model.Object               `json:"object"`
	Redundancy models.RedundancyDataTypes `json:"redundancy"`
}

func NewPreMoveObjectToStorageRespBody[T models.RedundancyDataTypes](nodeID int64, dir string, object model.Object, redundancy T) PreMoveObjectToStorageResp {
	return PreMoveObjectToStorageResp{
		NodeID:     nodeID,
		Directory:  dir,
		Object:     object,
		Redundancy: redundancy,
	}
}

// 调度完成
type MoveObjectToStorage struct {
	ObjectID  int64 `json:"objectID"`
	StorageID int64 `json:"storageID"`
	UserID    int64 `json:"userID"`
}

func NewMoveObjectToStorage(objectID int64, stgID int64, userID int64) MoveObjectToStorage {
	return MoveObjectToStorage{
		ObjectID:  objectID,
		StorageID: stgID,
		UserID:    userID,
	}
}

// 协调端发给客户端，告知要调度数据的详情
type MoveObjectToStorageResp struct{}

func NewMoveObjectToStorageResp() MoveObjectToStorageResp {
	return MoveObjectToStorageResp{}
}

func init() {
	mq.RegisterMessage[GetStorageInfo]()
	mq.RegisterMessage[GetStorageInfoResp]()

	mq.RegisterMessage[PreMoveObjectToStorage]()
	mq.RegisterMessage[PreMoveObjectToStorageResp]()

	mq.RegisterMessage[MoveObjectToStorage]()
	mq.RegisterMessage[MoveObjectToStorageResp]()
}
