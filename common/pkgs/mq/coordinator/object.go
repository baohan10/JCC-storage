package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type ObjectService interface {
	GetPackageObjects(msg *GetPackageObjects) (*GetPackageObjectsResp, *mq.CodeMessage)

	GetPackageObjectDetails(msg *GetPackageObjectDetails) (*GetPackageObjectDetailsResp, *mq.CodeMessage)

	GetObjectDetails(msg *GetObjectDetails) (*GetObjectDetailsResp, *mq.CodeMessage)

	UpdateObjectRedundancy(msg *UpdateObjectRedundancy) (*UpdateObjectRedundancyResp, *mq.CodeMessage)

	UpdateObjectInfos(msg *UpdateObjectInfos) (*UpdateObjectInfosResp, *mq.CodeMessage)

	MoveObjects(msg *MoveObjects) (*MoveObjectsResp, *mq.CodeMessage)

	DeleteObjects(msg *DeleteObjects) (*DeleteObjectsResp, *mq.CodeMessage)
}

// 查询Package中的所有Object，返回的Objects会按照ObjectID升序
var _ = Register(Service.GetPackageObjects)

type GetPackageObjects struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}
type GetPackageObjectsResp struct {
	mq.MessageBodyBase
	Objects []model.Object `json:"objects"`
}

func NewGetPackageObjects(userID cdssdk.UserID, packageID cdssdk.PackageID) *GetPackageObjects {
	return &GetPackageObjects{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewGetPackageObjectsResp(objects []model.Object) *GetPackageObjectsResp {
	return &GetPackageObjectsResp{
		Objects: objects,
	}
}
func (client *Client) GetPackageObjects(msg *GetPackageObjects) (*GetPackageObjectsResp, error) {
	return mq.Request(Service.GetPackageObjects, client.rabbitCli, msg)
}

// 获取Package中所有Object以及它们的分块详细信息，返回的Objects会按照ObjectID升序
var _ = Register(Service.GetPackageObjectDetails)

type GetPackageObjectDetails struct {
	mq.MessageBodyBase
	PackageID cdssdk.PackageID `json:"packageID"`
}
type GetPackageObjectDetailsResp struct {
	mq.MessageBodyBase
	Objects []stgmod.ObjectDetail `json:"objects"`
}

func NewGetPackageObjectDetails(packageID cdssdk.PackageID) *GetPackageObjectDetails {
	return &GetPackageObjectDetails{
		PackageID: packageID,
	}
}
func NewGetPackageObjectDetailsResp(objects []stgmod.ObjectDetail) *GetPackageObjectDetailsResp {
	return &GetPackageObjectDetailsResp{
		Objects: objects,
	}
}
func (client *Client) GetPackageObjectDetails(msg *GetPackageObjectDetails) (*GetPackageObjectDetailsResp, error) {
	return mq.Request(Service.GetPackageObjectDetails, client.rabbitCli, msg)
}

// 获取多个Object以及它们的分块详细信息，返回的Objects会按照ObjectID升序。
var _ = Register(Service.GetObjectDetails)

type GetObjectDetails struct {
	mq.MessageBodyBase
	ObjectIDs []cdssdk.ObjectID `json:"objectIDs"`
}
type GetObjectDetailsResp struct {
	mq.MessageBodyBase
	Objects []*stgmod.ObjectDetail `json:"objects"` // 如果没有查询到某个ID对应的信息，则此数组对应位置为nil
}

func ReqGetObjectDetails(objectIDs []cdssdk.ObjectID) *GetObjectDetails {
	return &GetObjectDetails{
		ObjectIDs: objectIDs,
	}
}
func RespGetObjectDetails(objects []*stgmod.ObjectDetail) *GetObjectDetailsResp {
	return &GetObjectDetailsResp{
		Objects: objects,
	}
}
func (client *Client) GetObjectDetails(msg *GetObjectDetails) (*GetObjectDetailsResp, error) {
	return mq.Request(Service.GetObjectDetails, client.rabbitCli, msg)
}

// 更新Object的冗余方式
var _ = Register(Service.UpdateObjectRedundancy)

type UpdateObjectRedundancy struct {
	mq.MessageBodyBase
	Updatings []UpdatingObjectRedundancy `json:"updatings"`
}
type UpdateObjectRedundancyResp struct {
	mq.MessageBodyBase
}
type UpdatingObjectRedundancy struct {
	ObjectID   cdssdk.ObjectID      `json:"objectID" db:"ObjectID"`
	Redundancy cdssdk.Redundancy    `json:"redundancy" db:"Redundancy"`
	PinnedAt   []cdssdk.NodeID      `json:"pinnedAt"`
	Blocks     []stgmod.ObjectBlock `json:"blocks"`
}

func ReqUpdateObjectRedundancy(updatings []UpdatingObjectRedundancy) *UpdateObjectRedundancy {
	return &UpdateObjectRedundancy{
		Updatings: updatings,
	}
}
func RespUpdateObjectRedundancy() *UpdateObjectRedundancyResp {
	return &UpdateObjectRedundancyResp{}
}
func (client *Client) UpdateObjectRedundancy(msg *UpdateObjectRedundancy) (*UpdateObjectRedundancyResp, error) {
	return mq.Request(Service.UpdateObjectRedundancy, client.rabbitCli, msg)
}

// 更新Object元数据
var _ = Register(Service.UpdateObjectInfos)

type UpdateObjectInfos struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID           `json:"userID"`
	Updatings []cdssdk.UpdatingObject `json:"updatings"`
}

type UpdateObjectInfosResp struct {
	mq.MessageBodyBase
	Successes []cdssdk.ObjectID `json:"successes"`
}

func ReqUpdateObjectInfos(userID cdssdk.UserID, updatings []cdssdk.UpdatingObject) *UpdateObjectInfos {
	return &UpdateObjectInfos{
		UserID:    userID,
		Updatings: updatings,
	}
}
func RespUpdateObjectInfos(successes []cdssdk.ObjectID) *UpdateObjectInfosResp {
	return &UpdateObjectInfosResp{
		Successes: successes,
	}
}
func (client *Client) UpdateObjectInfos(msg *UpdateObjectInfos) (*UpdateObjectInfosResp, error) {
	return mq.Request(Service.UpdateObjectInfos, client.rabbitCli, msg)
}

// 移动Object
var _ = Register(Service.MoveObjects)

type MoveObjects struct {
	mq.MessageBodyBase
	UserID  cdssdk.UserID         `json:"userID"`
	Movings []cdssdk.MovingObject `json:"movings"`
}

type MoveObjectsResp struct {
	mq.MessageBodyBase
	Successes []cdssdk.ObjectID `json:"successes"`
}

func ReqMoveObjects(userID cdssdk.UserID, movings []cdssdk.MovingObject) *MoveObjects {
	return &MoveObjects{
		UserID:  userID,
		Movings: movings,
	}
}
func RespMoveObjects(successes []cdssdk.ObjectID) *MoveObjectsResp {
	return &MoveObjectsResp{
		Successes: successes,
	}
}
func (client *Client) MoveObjects(msg *MoveObjects) (*MoveObjectsResp, error) {
	return mq.Request(Service.MoveObjects, client.rabbitCli, msg)
}

// 删除Object
var _ = Register(Service.DeleteObjects)

type DeleteObjects struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID     `json:"userID"`
	ObjectIDs []cdssdk.ObjectID `json:"objectIDs"`
}

type DeleteObjectsResp struct {
	mq.MessageBodyBase
}

func ReqDeleteObjects(userID cdssdk.UserID, objectIDs []cdssdk.ObjectID) *DeleteObjects {
	return &DeleteObjects{
		UserID:    userID,
		ObjectIDs: objectIDs,
	}
}
func RespDeleteObjects() *DeleteObjectsResp {
	return &DeleteObjectsResp{}
}
func (client *Client) DeleteObjects(msg *DeleteObjects) (*DeleteObjectsResp, error) {
	return mq.Request(Service.DeleteObjects, client.rabbitCli, msg)
}
