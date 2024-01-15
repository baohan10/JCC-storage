package coordinator

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type StorageService interface {
	GetStorageInfo(msg *GetStorageInfo) (*GetStorageInfoResp, *mq.CodeMessage)

	StoragePackageLoaded(msg *StoragePackageLoaded) (*StoragePackageLoadedResp, *mq.CodeMessage)

	GetPackageLoadLogDetails(msg *GetPackageLoadLogDetails) (*GetPackageLoadLogDetailsResp, *mq.CodeMessage)
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
	UserID       cdssdk.UserID        `json:"userID"`
	StorageID    cdssdk.StorageID     `json:"storageID"`
	PackageID    cdssdk.PackageID     `json:"packageID"`
	PinnedBlocks []stgmod.ObjectBlock `json:"pinnedBlocks"`
}
type StoragePackageLoadedResp struct {
	mq.MessageBodyBase
}

func NewStoragePackageLoaded(userID cdssdk.UserID, stgID cdssdk.StorageID, packageID cdssdk.PackageID, pinnedBlocks []stgmod.ObjectBlock) *StoragePackageLoaded {
	return &StoragePackageLoaded{
		UserID:       userID,
		PackageID:    packageID,
		StorageID:    stgID,
		PinnedBlocks: pinnedBlocks,
	}
}
func NewStoragePackageLoadedResp() *StoragePackageLoadedResp {
	return &StoragePackageLoadedResp{}
}
func (client *Client) StoragePackageLoaded(msg *StoragePackageLoaded) (*StoragePackageLoadedResp, error) {
	return mq.Request(Service.StoragePackageLoaded, client.rabbitCli, msg)
}

// 查询Package的导入记录
var _ = Register(Service.GetPackageLoadLogDetails)

type GetPackageLoadLogDetails struct {
	mq.MessageBodyBase
	PackageID cdssdk.PackageID `json:"packageID"`
}
type GetPackageLoadLogDetailsResp struct {
	mq.MessageBodyBase
	Logs []PackageLoadLogDetail `json:"logs"`
}
type PackageLoadLogDetail struct {
	Storage    model.Storage `json:"storage"`
	UserID     cdssdk.UserID `json:"userID"`
	CreateTime time.Time     `json:"createTime"`
}

func ReqGetPackageLoadLogDetails(packageID cdssdk.PackageID) *GetPackageLoadLogDetails {
	return &GetPackageLoadLogDetails{
		PackageID: packageID,
	}
}
func RespGetPackageLoadLogDetails(logs []PackageLoadLogDetail) *GetPackageLoadLogDetailsResp {
	return &GetPackageLoadLogDetailsResp{
		Logs: logs,
	}
}
func (client *Client) GetPackageLoadLogDetails(msg *GetPackageLoadLogDetails) (*GetPackageLoadLogDetailsResp, error) {
	return mq.Request(Service.GetPackageLoadLogDetails, client.rabbitCli, msg)
}
