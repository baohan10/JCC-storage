package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
)

type CacheService interface {
	CachePackageMoved(msg *CachePackageMoved) (*CachePackageMovedResp, *mq.CodeMessage)
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
