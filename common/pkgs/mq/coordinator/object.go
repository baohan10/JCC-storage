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

	ChangeObjectRedundancy(msg *ChangeObjectRedundancy) (*ChangeObjectRedundancyResp, *mq.CodeMessage)
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

// 更新Object的冗余方式
var _ = Register(Service.ChangeObjectRedundancy)

type ChangeObjectRedundancy struct {
	mq.MessageBodyBase
	Entries []ChangeObjectRedundancyEntry `json:"entries"`
}
type ChangeObjectRedundancyResp struct {
	mq.MessageBodyBase
}
type ChangeObjectRedundancyEntry struct {
	ObjectID   cdssdk.ObjectID      `json:"objectID"`
	Redundancy cdssdk.Redundancy    `json:"redundancy"`
	Blocks     []stgmod.ObjectBlock `json:"blocks"`
}

func ReqChangeObjectRedundancy(entries []ChangeObjectRedundancyEntry) *ChangeObjectRedundancy {
	return &ChangeObjectRedundancy{
		Entries: entries,
	}
}
func RespChangeObjectRedundancy() *ChangeObjectRedundancyResp {
	return &ChangeObjectRedundancyResp{}
}
func (client *Client) ChangeObjectRedundancy(msg *ChangeObjectRedundancy) (*ChangeObjectRedundancyResp, error) {
	return mq.Request(Service.ChangeObjectRedundancy, client.rabbitCli, msg)
}
