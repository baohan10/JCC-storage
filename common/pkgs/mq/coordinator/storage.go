package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type StorageService interface {
	GetStorageInfo(msg *GetStorageInfo) (*GetStorageInfoResp, *mq.CodeMessage)

	StoragePackageLoaded(msg *StoragePackageLoaded) (*StoragePackageLoadedResp, *mq.CodeMessage)
}

// 获取Storage信息
var _ = Register(Service.GetStorageInfo)

type GetStorageInfo struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	StorageID cdssdk.StorageID `json:"storageID"`
}
type GetStorageInfoResp struct {
	mq.MessageBodyBase
	model.Storage
}

func NewGetStorageInfo(userID cdssdk.UserID, storageID cdssdk.StorageID) *GetStorageInfo {
	return &GetStorageInfo{
		UserID:    userID,
		StorageID: storageID,
	}
}
func NewGetStorageInfoResp(storageID cdssdk.StorageID, name string, nodeID cdssdk.NodeID, dir string, state string) *GetStorageInfoResp {
	return &GetStorageInfoResp{
		Storage: model.Storage{
			StorageID: storageID,
			Name:      name,
			NodeID:    nodeID,
			Directory: dir,
			State:     state,
		},
	}
}
func (client *Client) GetStorageInfo(msg *GetStorageInfo) (*GetStorageInfoResp, error) {
	return mq.Request(Service.GetStorageInfo, client.rabbitCli, msg)
}

// 提交调度记录
var _ = Register(Service.StoragePackageLoaded)

type StoragePackageLoaded struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
	StorageID cdssdk.StorageID `json:"storageID"`
}
type StoragePackageLoadedResp struct {
	mq.MessageBodyBase
}

func NewStoragePackageLoaded(userID cdssdk.UserID, packageID cdssdk.PackageID, stgID cdssdk.StorageID) *StoragePackageLoaded {
	return &StoragePackageLoaded{
		UserID:    userID,
		PackageID: packageID,
		StorageID: stgID,
	}
}
func NewStoragePackageLoadedResp() *StoragePackageLoadedResp {
	return &StoragePackageLoadedResp{}
}
func (client *Client) StoragePackageLoaded(msg *StoragePackageLoaded) (*StoragePackageLoadedResp, error) {
	return mq.Request(Service.StoragePackageLoaded, client.rabbitCli, msg)
}
