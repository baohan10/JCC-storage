package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type ObjectService interface {
	GetPackageObjects(msg *GetPackageObjects) (*GetPackageObjectsResp, *mq.CodeMessage)

	GetPackageObjectRepData(msg *GetPackageObjectRepData) (*GetPackageObjectRepDataResp, *mq.CodeMessage)

	GetPackageObjectECData(msg *GetPackageObjectECData) (*GetPackageObjectECDataResp, *mq.CodeMessage)
}

// 查询Package中的所有Object，返回的Objects会按照ObjectID升序
var _ = Register(Service.GetPackageObjects)

type GetPackageObjects struct {
	mq.MessageBodyBase
	UserID    int64 `json:"userID"`
	PackageID int64 `json:"packageID"`
}
type GetPackageObjectsResp struct {
	mq.MessageBodyBase
	Objects []model.Object `json:"objects"`
}

func NewGetPackageObjects(userID int64, packageID int64) *GetPackageObjects {
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

// 获取指定Object的Rep数据，返回的Objects会按照ObjectID升序
var _ = Register(Service.GetPackageObjectRepData)

type GetPackageObjectRepData struct {
	mq.MessageBodyBase
	PackageID int64 `json:"packageID"`
}
type GetPackageObjectRepDataResp struct {
	mq.MessageBodyBase
	Data []stgmod.ObjectRepData `json:"data"`
}

func NewGetPackageObjectRepData(packageID int64) *GetPackageObjectRepData {
	return &GetPackageObjectRepData{
		PackageID: packageID,
	}
}
func NewGetPackageObjectRepDataResp(data []stgmod.ObjectRepData) *GetPackageObjectRepDataResp {
	return &GetPackageObjectRepDataResp{
		Data: data,
	}
}
func (client *Client) GetPackageObjectRepData(msg *GetPackageObjectRepData) (*GetPackageObjectRepDataResp, error) {
	return mq.Request(Service.GetPackageObjectRepData, client.rabbitCli, msg)
}

// 获取指定Object的EC数据，返回的Objects会按照ObjectID升序
var _ = Register(Service.GetPackageObjectECData)

type GetPackageObjectECData struct {
	mq.MessageBodyBase
	PackageID int64 `json:"packageID"`
}
type GetPackageObjectECDataResp struct {
	mq.MessageBodyBase
	Data []stgmod.ObjectECData `json:"data"`
}

func NewGetPackageObjectECData(packageID int64) *GetPackageObjectECData {
	return &GetPackageObjectECData{
		PackageID: packageID,
	}
}
func NewGetPackageObjectECDataResp(data []stgmod.ObjectECData) *GetPackageObjectECDataResp {
	return &GetPackageObjectECDataResp{
		Data: data,
	}
}
func (client *Client) GetPackageObjectECData(msg *GetPackageObjectECData) (*GetPackageObjectECDataResp, error) {
	return mq.Request(Service.GetPackageObjectECData, client.rabbitCli, msg)
}
