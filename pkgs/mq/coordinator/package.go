package coordinator

import (
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type PackageService interface {
	GetPackage(msg *GetPackage) (*GetPackageResp, *mq.CodeMessage)

	GetPackageObjects(msg *GetPackageObjects) (*GetPackageObjectsResp, *mq.CodeMessage)

	CreatePackage(msg *CreatePackage) (*CreatePackageResp, *mq.CodeMessage)

	UpdateRepPackage(msg *UpdateRepPackage) (*UpdateRepPackageResp, *mq.CodeMessage)

	UpdateECPackage(msg *UpdateECPackage) (*UpdateECPackageResp, *mq.CodeMessage)

	DeletePackage(msg *DeletePackage) (*DeletePackageResp, *mq.CodeMessage)

	GetPackageCachedNodes(msg *GetPackageCachedNodes) (*GetPackageCachedNodesResp, *mq.CodeMessage)

	GetPackageLoadedNodes(msg *GetPackageLoadedNodes) (*GetPackageLoadedNodesResp, *mq.CodeMessage)
}

// 获取Package基本信息
var _ = Register(PackageService.GetPackage)

type GetPackage struct {
	UserID    int64 `json:"userID"`
	PackageID int64 `json:"packageID"`
}
type GetPackageResp struct {
	model.Package
}

func NewGetPackage(userID int64, packageID int64) GetPackage {
	return GetPackage{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewGetPackageResp(pkg model.Package) GetPackageResp {
	return GetPackageResp{
		Package: pkg,
	}
}
func (client *Client) GetPackage(msg GetPackage) (*GetPackageResp, error) {
	return mq.Request[GetPackageResp](client.rabbitCli, msg)
}

// 查询Package中的所有Object，返回的Objects会按照ObjectID升序
var _ = Register(PackageService.GetPackageObjects)

type GetPackageObjects struct {
	UserID    int64 `json:"userID"`
	PackageID int64 `json:"packageID"`
}
type GetPackageObjectsResp struct {
	Objects []model.Object `json:"objects"`
}

func NewGetPackageObjects(userID int64, packageID int64) GetPackageObjects {
	return GetPackageObjects{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewGetPackageObjectsResp(objects []model.Object) GetPackageObjectsResp {
	return GetPackageObjectsResp{
		Objects: objects,
	}
}
func (client *Client) GetPackageObjects(msg GetPackageObjects) (*GetPackageObjectsResp, error) {
	return mq.Request[GetPackageObjectsResp](client.rabbitCli, msg)
}

// 创建一个Package
var _ = Register(PackageService.CreatePackage)

type CreatePackage struct {
	UserID     int64                      `json:"userID"`
	BucketID   int64                      `json:"bucketID"`
	Name       string                     `json:"name"`
	Redundancy models.TypedRedundancyInfo `json:"redundancy"`
}
type CreatePackageResp struct {
	PackageID int64 `json:"packageID"`
}

func NewCreatePackage(userID int64, bucketID int64, name string, redundancy models.TypedRedundancyInfo) CreatePackage {
	return CreatePackage{
		UserID:     userID,
		BucketID:   bucketID,
		Name:       name,
		Redundancy: redundancy,
	}
}
func NewCreatePackageResp(packageID int64) CreatePackageResp {
	return CreatePackageResp{
		PackageID: packageID,
	}
}
func (client *Client) CreatePackage(msg CreatePackage) (*CreatePackageResp, error) {
	return mq.Request[CreatePackageResp](client.rabbitCli, msg)
}

// 更新Rep备份模式的Package
var _ = Register(PackageService.UpdateRepPackage)

type UpdateRepPackage struct {
	PackageID int64              `json:"packageID"`
	Adds      []AddRepObjectInfo `json:"objects"`
	Deletes   []int64            `json:"deletes"`
}
type UpdateRepPackageResp struct{}
type AddRepObjectInfo struct {
	Path     string  `json:"path"`
	Size     int64   `json:"size,string"`
	FileHash string  `json:"fileHash"`
	NodeIDs  []int64 `json:"nodeIDs"`
}

func NewUpdateRepPackage(packageID int64, adds []AddRepObjectInfo, deletes []int64) UpdateRepPackage {
	return UpdateRepPackage{
		PackageID: packageID,
		Adds:      adds,
		Deletes:   deletes,
	}
}
func NewUpdateRepPackageResp() UpdateRepPackageResp {
	return UpdateRepPackageResp{}
}
func NewAddRepObjectInfo(path string, size int64, fileHash string, nodeIDs []int64) AddRepObjectInfo {
	return AddRepObjectInfo{
		Path:     path,
		Size:     size,
		FileHash: fileHash,
		NodeIDs:  nodeIDs,
	}
}
func (client *Client) UpdateRepPackage(msg UpdateRepPackage) (*UpdateRepPackageResp, error) {
	return mq.Request[UpdateRepPackageResp](client.rabbitCli, msg)
}

// 更新EC备份模式的Package
var _ = Register(PackageService.UpdateECPackage)

type UpdateECPackage struct {
	PackageID int64             `json:"packageID"`
	Adds      []AddECObjectInfo `json:"objects"`
	Deletes   []int64           `json:"deletes"`
}
type UpdateECPackageResp struct{}
type AddECObjectInfo struct {
	Path       string   `json:"path"`
	Size       int64    `json:"size,string"`
	FileHashes []string `json:"fileHashes"`
	NodeIDs    []int64  `json:"nodeIDs"`
}

func NewUpdateECPackage(packageID int64, adds []AddECObjectInfo, deletes []int64) UpdateECPackage {
	return UpdateECPackage{
		PackageID: packageID,
		Adds:      adds,
		Deletes:   deletes,
	}
}
func NewUpdateECPackageResp() UpdateECPackageResp {
	return UpdateECPackageResp{}
}
func NewAddECObjectInfo(path string, size int64, fileHashes []string, nodeIDs []int64) AddECObjectInfo {
	return AddECObjectInfo{
		Path:       path,
		Size:       size,
		FileHashes: fileHashes,
		NodeIDs:    nodeIDs,
	}
}
func (client *Client) UpdateECPackage(msg UpdateECPackage) (*UpdateECPackageResp, error) {
	return mq.Request[UpdateECPackageResp](client.rabbitCli, msg)
}

// 删除对象
var _ = Register(PackageService.DeletePackage)

type DeletePackage struct {
	UserID    int64 `db:"userID"`
	PackageID int64 `db:"packageID"`
}
type DeletePackageResp struct{}

func NewDeletePackage(userID int64, packageID int64) DeletePackage {
	return DeletePackage{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewDeletePackageResp() DeletePackageResp {
	return DeletePackageResp{}
}
func (client *Client) DeletePackage(msg DeletePackage) (*DeletePackageResp, error) {
	return mq.Request[DeletePackageResp](client.rabbitCli, msg)
}

// 根据PackageID获取object分布情况
var _ = Register(PackageService.GetPackageCachedNodes)

type GetPackageCachedNodes struct {
	UserID    int64 `json:"userID"`
	PackageID int64 `json:"packageID"`
}

type GetPackageCachedNodesResp struct {
	NodeIDs        []int64 `json:"nodeIDs"`
	RedundancyType string  `json:"redundancyType"`
}

func NewGetPackageCachedNodes(userID int64, packageID int64) GetPackageCachedNodes {
	return GetPackageCachedNodes{
		UserID:    userID,
		PackageID: packageID,
	}
}

func NewGetPackageCachedNodesResp(nodeIDs []int64, redundancyType string) GetPackageCachedNodesResp {
	return GetPackageCachedNodesResp{
		NodeIDs:        nodeIDs,
		RedundancyType: redundancyType,
	}
}

func (client *Client) GetPackageCachedNodes(msg GetPackageCachedNodes) (*GetPackageCachedNodesResp, error) {
	return mq.Request[GetPackageCachedNodesResp](client.rabbitCli, msg)
}

// 根据PackageID获取storage分布情况
var _ = Register(PackageService.GetPackageLoadedNodes)

type GetPackageLoadedNodes struct {
	UserID    int64 `json:"userID"`
	PackageID int64 `json:"packageID"`
}

type GetPackageLoadedNodesResp struct {
	NodeIDs []int64 `json:"nodeIDs"`
}

func NewGetPackageLoadedNodes(userID int64, packageID int64) GetPackageLoadedNodes {
	return GetPackageLoadedNodes{
		UserID:    userID,
		PackageID: packageID,
	}
}

func NewGetPackageLoadedNodesResp(nodeIDs []int64) GetPackageLoadedNodesResp {
	return GetPackageLoadedNodesResp{
		NodeIDs: nodeIDs,
	}
}

func (client *Client) GetPackageLoadedNodes(msg GetPackageLoadedNodes) (*GetPackageLoadedNodesResp, error) {
	return mq.Request[GetPackageLoadedNodesResp](client.rabbitCli, msg)
}
