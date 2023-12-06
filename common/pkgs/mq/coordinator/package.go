package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type PackageService interface {
	GetPackage(msg *GetPackage) (*GetPackageResp, *mq.CodeMessage)

	CreatePackage(msg *CreatePackage) (*CreatePackageResp, *mq.CodeMessage)

	UpdateECPackage(msg *UpdatePackage) (*UpdatePackageResp, *mq.CodeMessage)

	DeletePackage(msg *DeletePackage) (*DeletePackageResp, *mq.CodeMessage)

	GetPackageCachedNodes(msg *GetPackageCachedNodes) (*GetPackageCachedNodesResp, *mq.CodeMessage)

	GetPackageLoadedNodes(msg *GetPackageLoadedNodes) (*GetPackageLoadedNodesResp, *mq.CodeMessage)
}

// 获取Package基本信息
var _ = Register(Service.GetPackage)

type GetPackage struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}
type GetPackageResp struct {
	mq.MessageBodyBase
	model.Package
}

func NewGetPackage(userID cdssdk.UserID, packageID cdssdk.PackageID) *GetPackage {
	return &GetPackage{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewGetPackageResp(pkg model.Package) *GetPackageResp {
	return &GetPackageResp{
		Package: pkg,
	}
}
func (client *Client) GetPackage(msg *GetPackage) (*GetPackageResp, error) {
	return mq.Request(Service.GetPackage, client.rabbitCli, msg)
}

// 创建一个Package
var _ = Register(Service.CreatePackage)

type CreatePackage struct {
	mq.MessageBodyBase
	UserID   cdssdk.UserID   `json:"userID"`
	BucketID cdssdk.BucketID `json:"bucketID"`
	Name     string          `json:"name"`
}
type CreatePackageResp struct {
	mq.MessageBodyBase
	PackageID cdssdk.PackageID `json:"packageID"`
}

func NewCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string) *CreatePackage {
	return &CreatePackage{
		UserID:   userID,
		BucketID: bucketID,
		Name:     name,
	}
}
func NewCreatePackageResp(packageID cdssdk.PackageID) *CreatePackageResp {
	return &CreatePackageResp{
		PackageID: packageID,
	}
}
func (client *Client) CreatePackage(msg *CreatePackage) (*CreatePackageResp, error) {
	return mq.Request(Service.CreatePackage, client.rabbitCli, msg)
}

// 更新EC备份模式的Package
var _ = Register(Service.UpdateECPackage)

type UpdatePackage struct {
	mq.MessageBodyBase
	PackageID cdssdk.PackageID  `json:"packageID"`
	Adds      []AddObjectInfo   `json:"objects"`
	Deletes   []cdssdk.ObjectID `json:"deletes"`
}
type UpdatePackageResp struct {
	mq.MessageBodyBase
}
type AddObjectInfo struct {
	Path     string        `json:"path"`
	Size     int64         `json:"size,string"`
	FileHash string        `json:"fileHash"`
	NodeID   cdssdk.NodeID `json:"nodeID"`
}

func NewUpdatePackage(packageID cdssdk.PackageID, adds []AddObjectInfo, deletes []cdssdk.ObjectID) *UpdatePackage {
	return &UpdatePackage{
		PackageID: packageID,
		Adds:      adds,
		Deletes:   deletes,
	}
}
func NewUpdatePackageResp() *UpdatePackageResp {
	return &UpdatePackageResp{}
}
func NewAddObjectInfo(path string, size int64, fileHash string, nodeIDs cdssdk.NodeID) AddObjectInfo {
	return AddObjectInfo{
		Path:     path,
		Size:     size,
		FileHash: fileHash,
		NodeID:   nodeIDs,
	}
}
func (client *Client) UpdateECPackage(msg *UpdatePackage) (*UpdatePackageResp, error) {
	return mq.Request(Service.UpdateECPackage, client.rabbitCli, msg)
}

// 删除对象
var _ = Register(Service.DeletePackage)

type DeletePackage struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `db:"userID"`
	PackageID cdssdk.PackageID `db:"packageID"`
}
type DeletePackageResp struct {
	mq.MessageBodyBase
}

func NewDeletePackage(userID cdssdk.UserID, packageID cdssdk.PackageID) *DeletePackage {
	return &DeletePackage{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewDeletePackageResp() *DeletePackageResp {
	return &DeletePackageResp{}
}
func (client *Client) DeletePackage(msg *DeletePackage) (*DeletePackageResp, error) {
	return mq.Request(Service.DeletePackage, client.rabbitCli, msg)
}

// 根据PackageID获取object分布情况
var _ = Register(Service.GetPackageCachedNodes)

type GetPackageCachedNodes struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}

type PackageCachedNodeInfo struct {
	NodeID      int64 `json:"nodeID"`
	FileSize    int64 `json:"fileSize"`
	ObjectCount int64 `json:"objectCount"`
}

type GetPackageCachedNodesResp struct {
	mq.MessageBodyBase
	cdssdk.PackageCachingInfo
}

func NewGetPackageCachedNodes(userID cdssdk.UserID, packageID cdssdk.PackageID) *GetPackageCachedNodes {
	return &GetPackageCachedNodes{
		UserID:    userID,
		PackageID: packageID,
	}
}

func NewGetPackageCachedNodesResp(nodeInfos []cdssdk.NodePackageCachingInfo, packageSize int64) *GetPackageCachedNodesResp {
	return &GetPackageCachedNodesResp{
		PackageCachingInfo: cdssdk.PackageCachingInfo{
			NodeInfos:   nodeInfos,
			PackageSize: packageSize,
		},
	}
}

func (client *Client) GetPackageCachedNodes(msg *GetPackageCachedNodes) (*GetPackageCachedNodesResp, error) {
	return mq.Request(Service.GetPackageCachedNodes, client.rabbitCli, msg)
}

// 根据PackageID获取storage分布情况
var _ = Register(Service.GetPackageLoadedNodes)

type GetPackageLoadedNodes struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}

type GetPackageLoadedNodesResp struct {
	mq.MessageBodyBase
	NodeIDs []cdssdk.NodeID `json:"nodeIDs"`
}

func NewGetPackageLoadedNodes(userID cdssdk.UserID, packageID cdssdk.PackageID) *GetPackageLoadedNodes {
	return &GetPackageLoadedNodes{
		UserID:    userID,
		PackageID: packageID,
	}
}

func NewGetPackageLoadedNodesResp(nodeIDs []cdssdk.NodeID) *GetPackageLoadedNodesResp {
	return &GetPackageLoadedNodesResp{
		NodeIDs: nodeIDs,
	}
}

func (client *Client) GetPackageLoadedNodes(msg *GetPackageLoadedNodes) (*GetPackageLoadedNodesResp, error) {
	return mq.Request(Service.GetPackageLoadedNodes, client.rabbitCli, msg)
}
