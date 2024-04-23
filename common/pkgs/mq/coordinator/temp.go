package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

// 删除Object
var _ = Register(Service.GetDatabaseAll)

type GetDatabaseAll struct {
	mq.MessageBodyBase
	UserID cdssdk.UserID `json:"userID"`
}

type GetDatabaseAllResp struct {
	mq.MessageBodyBase
	Buckets  []cdssdk.Bucket       `json:"buckets"`
	Packages []cdssdk.Package      `json:"packages"`
	Objects  []stgmod.ObjectDetail `json:"objects"`
}

func ReqGetDatabaseAll(userID cdssdk.UserID) *GetDatabaseAll {
	return &GetDatabaseAll{
		UserID: userID,
	}
}
func RespGetDatabaseAll(buckets []cdssdk.Bucket, packages []cdssdk.Package, objects []stgmod.ObjectDetail) *GetDatabaseAllResp {
	return &GetDatabaseAllResp{
		Buckets:  buckets,
		Packages: packages,
		Objects:  objects,
	}
}
func (client *Client) GetDatabaseAll(msg *GetDatabaseAll) (*GetDatabaseAllResp, error) {
	return mq.Request(Service.GetDatabaseAll, client.rabbitCli, msg)
}
