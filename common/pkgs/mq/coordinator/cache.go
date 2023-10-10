package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgsdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type CacheService interface {
	CachePackageMoved(msg *CachePackageMoved) (*CachePackageMovedResp, *mq.CodeMessage)

	GetPackageObjectCacheInfos(msg *GetPackageObjectCacheInfos) (*GetPackageObjectCacheInfosResp, *mq.CodeMessage)
}

// Package的Object移动到了节点的Cache中
var _ = Register(Service.CachePackageMoved)

type CachePackageMoved struct {
	mq.MessageBodyBase
	PackageID  int64    `json:"packageID"`
	NodeID     int64    `json:"nodeID"`
	FileHashes []string `json:"fileHashes"`
}
type CachePackageMovedResp struct {
	mq.MessageBodyBase
}

func NewCachePackageMoved(packageID int64, nodeID int64, fileHashes []string) *CachePackageMoved {
	return &CachePackageMoved{
		PackageID:  packageID,
		NodeID:     nodeID,
		FileHashes: fileHashes,
	}
}
func NewCachePackageMovedResp() *CachePackageMovedResp {
	return &CachePackageMovedResp{}
}
func (client *Client) CachePackageMoved(msg *CachePackageMoved) (*CachePackageMovedResp, error) {
	return mq.Request(Service.CachePackageMoved, client.rabbitCli, msg)
}

// 获取Package中所有Object的FileHash
var _ = Register(Service.GetPackageObjectCacheInfos)

type GetPackageObjectCacheInfos struct {
	mq.MessageBodyBase
	UserID    int64 `json:"userID"`
	PackageID int64 `json:"packageID"`
}
type GetPackageObjectCacheInfosResp struct {
	mq.MessageBodyBase
	Infos []stgsdk.ObjectCacheInfo
}

func NewGetPackageObjectCacheInfos(userID int64, packageID int64) *GetPackageObjectCacheInfos {
	return &GetPackageObjectCacheInfos{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewGetPackageObjectCacheInfosResp(infos []stgsdk.ObjectCacheInfo) *GetPackageObjectCacheInfosResp {
	return &GetPackageObjectCacheInfosResp{
		Infos: infos,
	}
}
func (client *Client) GetPackageObjectCacheInfos(msg *GetPackageObjectCacheInfos) (*GetPackageObjectCacheInfosResp, error) {
	return mq.Request(Service.GetPackageObjectCacheInfos, client.rabbitCli, msg)
}
