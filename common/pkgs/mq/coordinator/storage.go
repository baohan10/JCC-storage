package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type StorageService interface {
	GetStorageInfo(msg *GetStorageInfo) (*GetStorageInfoResp, *mq.CodeMessage)

	StoragePackageLoaded(msg *StoragePackageLoaded) (*StoragePackageLoadedResp, *mq.CodeMessage)
}

// 获取Storage信息
var _ = Register(StorageService.GetStorageInfo)

type GetStorageInfo struct {
	UserID    int64 `json:"userID"`
	StorageID int64 `json:"storageID"`
}
type GetStorageInfoResp struct {
	model.Storage
}

func NewGetStorageInfo(userID int64, storageID int64) GetStorageInfo {
	return GetStorageInfo{
		UserID:    userID,
		StorageID: storageID,
	}
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
func (client *Client) GetStorageInfo(msg GetStorageInfo) (*GetStorageInfoResp, error) {
	return mq.Request[GetStorageInfoResp](client.rabbitCli, msg)
}

// 提交调度记录
var _ = Register(StorageService.StoragePackageLoaded)

type StoragePackageLoaded struct {
	UserID    int64 `json:"userID"`
	PackageID int64 `json:"packageID"`
	StorageID int64 `json:"storageID"`
}
type StoragePackageLoadedResp struct{}

func NewStoragePackageLoaded(userID int64, packageID int64, stgID int64) StoragePackageLoaded {
	return StoragePackageLoaded{
		UserID:    userID,
		PackageID: packageID,
		StorageID: stgID,
	}
}
func NewStoragePackageLoadedResp() StoragePackageLoadedResp {
	return StoragePackageLoadedResp{}
}
func (client *Client) StoragePackageLoaded(msg StoragePackageLoaded) (*StoragePackageLoadedResp, error) {
	return mq.Request[StoragePackageLoadedResp](client.rabbitCli, msg)
}
