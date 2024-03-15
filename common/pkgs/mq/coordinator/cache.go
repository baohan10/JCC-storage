package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type CacheService interface {
	CachePackageMoved(msg *CachePackageMoved) (*CachePackageMovedResp, *mq.CodeMessage)

	CacheRemovePackage(msg *CacheRemovePackage) (*CacheRemovePackageResp, *mq.CodeMessage)
}

// Package的Object移动到了节点的Cache中
var _ = Register(Service.CachePackageMoved)

type CachePackageMoved struct {
	mq.MessageBodyBase
	PackageID cdssdk.PackageID `json:"packageID"`
	NodeID    cdssdk.NodeID    `json:"nodeID"`
}
type CachePackageMovedResp struct {
	mq.MessageBodyBase
}

func NewCachePackageMoved(packageID cdssdk.PackageID, nodeID cdssdk.NodeID) *CachePackageMoved {
	return &CachePackageMoved{
		PackageID: packageID,
		NodeID:    nodeID,
	}
}
func NewCachePackageMovedResp() *CachePackageMovedResp {
	return &CachePackageMovedResp{}
}
func (client *Client) CachePackageMoved(msg *CachePackageMoved) (*CachePackageMovedResp, error) {
	return mq.Request(Service.CachePackageMoved, client.rabbitCli, msg)
}

// 删除移动到指定节点Cache中的Package
var _ = Register(Service.CacheRemovePackage)

type CacheRemovePackage struct {
	mq.MessageBodyBase
	PackageID cdssdk.PackageID `json:"packageID"`
	NodeID    cdssdk.NodeID    `json:"nodeID"`
}
type CacheRemovePackageResp struct {
	mq.MessageBodyBase
}

func ReqCacheRemoveMovedPackage(packageID cdssdk.PackageID, nodeID cdssdk.NodeID) *CacheRemovePackage {
	return &CacheRemovePackage{
		PackageID: packageID,
		NodeID:    nodeID,
	}
}
func RespCacheRemovePackage() *CacheRemovePackageResp {
	return &CacheRemovePackageResp{}
}
func (client *Client) CacheRemovePackage(msg *CacheRemovePackage) (*CacheRemovePackageResp, error) {
	return mq.Request(Service.CacheRemovePackage, client.rabbitCli, msg)
}
