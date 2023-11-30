package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type ObjectService interface {
	GetPackageObjects(msg *GetPackageObjects) (*GetPackageObjectsResp, *mq.CodeMessage)

	GetPackageObjectECData(msg *GetPackageObjectECData) (*GetPackageObjectECDataResp, *mq.CodeMessage)
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

// 获取指定Object的EC数据，返回的Objects会按照ObjectID升序
var _ = Register(Service.GetPackageObjectECData)

type GetPackageObjectECData struct {
	mq.MessageBodyBase
	PackageID cdssdk.PackageID `json:"packageID"`
}
type GetPackageObjectECDataResp struct {
	mq.MessageBodyBase
	Data []stgmod.ObjectECData `json:"data"`
}

func NewGetPackageObjectECData(packageID cdssdk.PackageID) *GetPackageObjectECData {
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
