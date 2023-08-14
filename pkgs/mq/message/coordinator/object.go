package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	ramsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message"
)

// 客户端发给协调端，告知要查询的文件夹名称
type GetObjectsByDirName struct {
	UserID  int64  `json:"userID"`
	DirName string `json:"dirName"`
}

func NewGetObjectsByDirName(userID int64, dirName string) GetObjectsByDirName {
	return GetObjectsByDirName{
		UserID:  userID,
		DirName: dirName,
	}
}

// 协调端告知客户端，查询到的object信息
type GetObjectsResp struct {
	Objects []model.Object `json:"objects"`
}

func NewGetObjectsResp(objects []model.Object) GetObjectsResp {
	return GetObjectsResp{
		Objects: objects,
	}
}

// 客户端发给协调端，告知要读取数据
type PreDownloadObject struct {
	ObjectID         int64  `json:"objectID"`
	UserID           int64  `json:"userID"`
	ClientExternalIP string `json:"clientExternalIP"` // 客户端的外网IP
}

func NewPreDownloadObject(objectID int64, userID int64, clientExternalIP string) PreDownloadObject {
	return PreDownloadObject{
		ObjectID:         objectID,
		UserID:           userID,
		ClientExternalIP: clientExternalIP,
	}
}

// 协调端告知客户端，待读取数据的元数据
type PreDownloadObjectResp struct {
	FileSize   int64                         `json:"fileSize,string"`
	Redundancy ramsg.RespRedundancyDataTypes `json:"redundancy"`
}

func NewPreDownloadObjectResp[T ramsg.RespRedundancyDataTypesConst](fileSize int64, redundancy T) PreDownloadObjectResp {
	return PreDownloadObjectResp{
		Redundancy: redundancy,
		FileSize:   fileSize,
	}
}

// 客户端发给协调端，告知要以多副本方式执行写入操作
type PreUploadRepObject struct {
	BucketID         int64  `json:"bucketID"`
	ObjectName       string `json:"objectName"`
	FileSize         int64  `json:"fileSize,string"`
	UserID           int64  `json:"userID"`
	ClientExternalIP string `json:"clientExternalIP"` // 客户端的外网IP
}

func NewPreUploadRepObjectBody(bucketID int64, objectName string, fileSize int64, userID int64, clientExterIP string) PreUploadRepObject {
	return PreUploadRepObject{
		BucketID:         bucketID,
		ObjectName:       objectName,
		FileSize:         fileSize,
		UserID:           userID,
		ClientExternalIP: clientExterIP,
	}
}

// 协调端发给客户端，返回副本的写入目的地节点IP
type PreUploadResp struct {
	Nodes []ramsg.RespNode `json:"nodes"`
}

func NewPreUploadResp(nodes []ramsg.RespNode) PreUploadResp {
	return PreUploadResp{
		Nodes: nodes,
	}
}

// 客户端发给协调端，告知要以纠删码方式执行写入操作
type PreUploadEcObject struct {
	BucketID         int64  `json:"bucketID"`
	ObjectName       string `json:"objectName"`
	FileSize         int64  `json:"fileSize,string"`
	EcName           string `json:"ecName"`
	UserID           int64  `json:"userID"`
	ClientExternalIP string `json:"clientExternalIP"` // 读取方的外网IP
}

func NewPreUploadEcObject(bucketID int64, objectName string, fileSize int64, ecName string, userID int64, writerExterIP string) PreUploadEcObject {
	return PreUploadEcObject{
		BucketID:         bucketID,
		ObjectName:       objectName,
		FileSize:         fileSize,
		EcName:           ecName,
		UserID:           userID,
		ClientExternalIP: writerExterIP,
	}
}

// 协调端发给客户端，返回编码块的写入目的地节点IP
type PreUploadEcResp struct {
	Nodes []ramsg.RespNode `json:"nodes"`
	Ec    ramsg.Ec         `json:"ec"`
}

func NewPreUploadEcResp(nodes []ramsg.RespNode, ec ramsg.Ec) PreUploadEcResp {
	return PreUploadEcResp{
		Nodes: nodes,
		Ec:    ec,
	}
}

type CreateRepObject struct {
	BucketID   int64   `json:"bucketID"`
	ObjectName string  `json:"objectName"`
	NodeIDs    []int64 `json:"nodeIDs"`
	FileHash   string  `json:"fileHash"`
	FileSize   int64   `json:"fileSize,string"`
	RepCount   int     `json:"repCount"`
	UserID     int64   `json:"userID"`
	DirName    string  `json:"dirName"`
}

func NewCreateRepObject(bucketID int64, objectName string, fileSize int64, repCount int, userID int64, nodeIDs []int64, fileHash string, dirName string) CreateRepObject {
	return CreateRepObject{
		BucketID:   bucketID,
		ObjectName: objectName,
		FileSize:   fileSize,
		RepCount:   repCount,
		UserID:     userID,
		NodeIDs:    nodeIDs,
		FileHash:   fileHash,
	}
}

type CreateEcObject struct {
	BucketID   int64    `json:"bucketID"`
	ObjectName string   `json:"objectName"`
	NodeIDs    []int64  `json:"nodeIDs"`
	Hashes     []string `json:"hashes"`
	FileSize   int64    `json:"fileSize,string"`
	UserID     int64    `json:"userID"`
	EcName     string   `json:"ecName"`
	DirName    string   `json:"dirName"`
}

func NewCreateEcObject(bucketID int64, objectName string, fileSize int64, userID int64, nodeIDs []int64, hashes []string, ecName string, dirName string) CreateEcObject {
	return CreateEcObject{
		BucketID:   bucketID,
		ObjectName: objectName,
		FileSize:   fileSize,
		UserID:     userID,
		NodeIDs:    nodeIDs,
		Hashes:     hashes,
		EcName:     ecName,
		DirName:    dirName,
	}
}

// 协调端发给客户端，告知哈希写入结果
type CreateObjectResp struct {
	ObjectID int64 `json:"objectID"`
}

func NewCreateObjectResp(objectID int64) CreateObjectResp {
	return CreateObjectResp{
		ObjectID: objectID,
	}
}

// PreUpdateRepObject 更新Rep对象
type PreUpdateRepObject struct {
	ObjectID         int64  `json:"objectID"`
	FileSize         int64  `json:"fileSize,string"`
	UserID           int64  `json:"userID"`
	ClientExternalIP string `json:"clientExternalIP"`
}

func NewPreUpdateRepObject(objectID int64, fileSize int64, userID int64, clientExternalIP string) PreUpdateRepObject {
	return PreUpdateRepObject{
		ObjectID:         objectID,
		FileSize:         fileSize,
		UserID:           userID,
		ClientExternalIP: clientExternalIP,
	}
}

type PreUpdateRepObjectResp struct {
	Nodes []PreUpdateRepObjectRespNode `json:"nodes"`
}
type PreUpdateRepObjectRespNode struct {
	ID             int64  `json:"id"`
	ExternalIP     string `json:"externalIP"`
	LocalIP        string `json:"localIP"`
	IsSameLocation bool   `json:"isSameLocation"` // 客户端是否与此节点在同一个地域
	HasOldObject   bool   `json:"hasOldObject"`   // 节点是否存有旧的对象文件
}

func NewPreUpdateRepObjectRespNode(id int64, exterIP string, localIP string, isSameLocation bool, hasOldObject bool) PreUpdateRepObjectRespNode {
	return PreUpdateRepObjectRespNode{
		ID:             id,
		ExternalIP:     exterIP,
		LocalIP:        localIP,
		IsSameLocation: isSameLocation,
		HasOldObject:   hasOldObject,
	}
}

func NewPreUpdateRepObjectResp(nodes []PreUpdateRepObjectRespNode) PreUpdateRepObjectResp {
	return PreUpdateRepObjectResp{
		Nodes: nodes,
	}
}

// UpdateRepObject 更新Rep对象
type UpdateRepObject struct {
	ObjectID int64   `json:"objectID"`
	FileHash string  `json:"fileHash"`
	FileSize int64   `json:"fileSize,string"`
	NodeIDs  []int64 `json:"nodeIDs"`
	UserID   int64   `json:"userID"`
}

func NewUpdateRepObject(objectID int64, fileHash string, fileSize int64, nodeIDs []int64, userID int64) UpdateRepObject {
	return UpdateRepObject{
		ObjectID: objectID,
		FileHash: fileHash,
		FileSize: fileSize,
		NodeIDs:  nodeIDs,
		UserID:   userID,
	}
}

type UpdateRepObjectResp struct{}

func NewUpdateRepObjectResp() UpdateRepObjectResp {
	return UpdateRepObjectResp{}
}

// DeleteObjectBody 删除对象
type DeleteObject struct {
	UserID   int64 `db:"userID"`
	ObjectID int64 `db:"objectID"`
}

func NewDeleteObject(userID int64, objectID int64) DeleteObject {
	return DeleteObject{
		UserID:   userID,
		ObjectID: objectID,
	}
}

type DeleteObjectResp struct{}

func NewDeleteObjectResp() DeleteObjectResp {
	return DeleteObjectResp{}
}

func init() {
	mq.RegisterMessage[GetObjectsByDirName]()
	mq.RegisterMessage[GetObjectsResp]()

	mq.RegisterMessage[PreDownloadObject]()
	mq.RegisterMessage[PreDownloadObjectResp]()

	mq.RegisterMessage[PreUploadRepObject]()
	mq.RegisterMessage[PreUploadResp]()

	mq.RegisterMessage[PreUploadEcObject]()
	mq.RegisterMessage[PreUploadEcResp]()

	mq.RegisterMessage[CreateRepObject]()
	mq.RegisterMessage[CreateEcObject]()
	mq.RegisterMessage[CreateObjectResp]()

	mq.RegisterMessage[PreUpdateRepObject]()
	mq.RegisterMessage[PreUpdateRepObjectResp]()
	mq.RegisterMessage[UpdateRepObject]()
	mq.RegisterMessage[UpdateRepObjectResp]()

	mq.RegisterMessage[DeleteObject]()
	mq.RegisterMessage[DeleteObjectResp]()
}
