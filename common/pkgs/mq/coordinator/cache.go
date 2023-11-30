package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type CacheService interface {
	CachePackageMoved(msg *CachePackageMoved) (*CachePackageMovedResp, *mq.CodeMessage)

	GetPackageObjectCacheInfos(msg *GetPackageObjectCacheInfos) (*GetPackageObjectCacheInfosResp, *mq.CodeMessage)
}

// Package的Object移动到了节点的Cache中
var _ = Register(Service.CachePackageMoved)

type CachePackageMoved struct {
	mq.MessageBodyBase
	PackageID  cdssdk.PackageID `json:"packageID"`
	NodeID     cdssdk.NodeID    `json:"nodeID"`
	FileHashes []string         `json:"fileHashes"`
}
type CachePackageMovedResp struct {
	mq.MessageBodyBase
}

func NewCachePackageMoved(packageID cdssdk.PackageID, nodeID cdssdk.NodeID, fileHashes []string) *CachePackageMoved {
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
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}
type GetPackageObjectCacheInfosResp struct {
	mq.MessageBodyBase
	Infos []cdssdk.ObjectCacheInfo
}

func NewGetPackageObjectCacheInfos(userID cdssdk.UserID, packageID cdssdk.PackageID) *GetPackageObjectCacheInfos {
	return &GetPackageObjectCacheInfos{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewGetPackageObjectCacheInfosResp(infos []cdssdk.ObjectCacheInfo) *GetPackageObjectCacheInfosResp {
	return &GetPackageObjectCacheInfosResp{
		Infos: infos,
	}
}
func (client *Client) GetPackageObjectCacheInfos(msg *GetPackageObjectCacheInfos) (*GetPackageObjectCacheInfosResp, error) {
	return mq.Request(Service.GetPackageObjectCacheInfos, client.rabbitCli, msg)
}
