package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type CommonService interface {
	FindClientLocation(msg *FindClientLocation) (*FindClientLocationResp, *mq.CodeMessage)

	GetECConfig(msg *GetECConfig) (*GetECConfigResp, *mq.CodeMessage)
}

// 查询指定IP所属的地域
var _ = Register(Service.FindClientLocation)

type FindClientLocation struct {
	mq.MessageBodyBase
	IP string `json:"ip"`
}
type FindClientLocationResp struct {
	mq.MessageBodyBase
	Location model.Location `json:"location"`
}

func NewFindClientLocation(ip string) *FindClientLocation {
	return &FindClientLocation{
		IP: ip,
	}
}
func NewFindClientLocationResp(location model.Location) *FindClientLocationResp {
	return &FindClientLocationResp{
		Location: location,
	}
}
func (client *Client) FindClientLocation(msg *FindClientLocation) (*FindClientLocationResp, error) {
	return mq.Request(Service.FindClientLocation, client.rabbitCli, msg)
}

// 获取EC具体配置
var _ = Register(Service.GetECConfig)

type GetECConfig struct {
	mq.MessageBodyBase
	ECName string `json:"ecName"`
}
type GetECConfigResp struct {
	mq.MessageBodyBase
	Config model.Ec `json:"config"`
}

func NewGetECConfig(ecName string) *GetECConfig {
	return &GetECConfig{
		ECName: ecName,
	}
}
func NewGetECConfigResp(config model.Ec) *GetECConfigResp {
	return &GetECConfigResp{
		Config: config,
	}
}
func (client *Client) GetECConfig(msg *GetECConfig) (*GetECConfigResp, error) {
	return mq.Request(Service.GetECConfig, client.rabbitCli, msg)
}
