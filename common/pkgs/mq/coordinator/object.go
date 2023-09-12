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
var _ = Register(Service.GetPackageObjectRepData)

type GetPackageObjectRepData struct {
	mq.MessageBodyBase
	PackageID int64 `json:"packageID"`
}
type GetPackageObjectRepDataResp struct {
	mq.MessageBodyBase
	Data []models.ObjectRepData `json:"data"`
}

func NewGetPackageObjectRepData(packageID int64) *GetPackageObjectRepData {
	return &GetPackageObjectRepData{
		PackageID: packageID,
	}
}
func NewGetPackageObjectRepDataResp(data []models.ObjectRepData) *GetPackageObjectRepDataResp {
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
	Data []models.ObjectECData `json:"data"`
}

func NewGetPackageObjectECData(packageID int64) *GetPackageObjectECData {
	return &GetPackageObjectECData{
		PackageID: packageID,
	}
}
func NewGetPackageObjectECDataResp(data []models.ObjectECData) *GetPackageObjectECDataResp {
	return &GetPackageObjectECDataResp{
		Data: data,
	}
}
func (client *Client) GetPackageObjectECData(msg *GetPackageObjectECData) (*GetPackageObjectECDataResp, error) {
	return mq.Request(Service.GetPackageObjectECData, client.rabbitCli, msg)
}
