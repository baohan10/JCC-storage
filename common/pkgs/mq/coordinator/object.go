package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/common/models"
)

type ObjectService interface {
	GetPackageObjectRepData(msg *GetPackageObjectRepData) (*GetPackageObjectRepDataResp, *mq.CodeMessage)

	GetPackageObjectECData(msg *GetPackageObjectECData) (*GetPackageObjectECDataResp, *mq.CodeMessage)
}

// 获取指定Object的Rep数据，返回的Objects会按照ObjectID升序
var _ = Register(ObjectService.GetPackageObjectRepData)

type GetPackageObjectRepData struct {
	PackageID int64 `json:"packageID"`
}
type GetPackageObjectRepDataResp struct {
	Data []models.ObjectRepData `json:"data"`
}

func NewGetPackageObjectRepData(packageID int64) GetPackageObjectRepData {
	return GetPackageObjectRepData{
		PackageID: packageID,
	}
}
func NewGetPackageObjectRepDataResp(data []models.ObjectRepData) GetPackageObjectRepDataResp {
	return GetPackageObjectRepDataResp{
		Data: data,
	}
}
func (client *Client) GetPackageObjectRepData(msg GetPackageObjectRepData) (*GetPackageObjectRepDataResp, error) {
	return mq.Request[GetPackageObjectRepDataResp](client.rabbitCli, msg)
}

// 获取指定Object的EC数据，返回的Objects会按照ObjectID升序
var _ = Register(ObjectService.GetPackageObjectECData)

type GetPackageObjectECData struct {
	PackageID int64 `json:"packageID"`
}
type GetPackageObjectECDataResp struct {
	Data []models.ObjectECData `json:"data"`
}

func NewGetPackageObjectECData(packageID int64) GetPackageObjectECData {
	return GetPackageObjectECData{
		PackageID: packageID,
	}
}
func NewGetPackageObjectECDataResp(data []models.ObjectECData) GetPackageObjectECDataResp {
	return GetPackageObjectECDataResp{
		Data: data,
	}
}
func (client *Client) GetPackageObjectECData(msg GetPackageObjectECData) (*GetPackageObjectECDataResp, error) {
	return mq.Request[GetPackageObjectECDataResp](client.rabbitCli, msg)
}
