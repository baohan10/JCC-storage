package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
)

type AgentService interface {
	GetState(msg *GetState) (*GetStateResp, *mq.CodeMessage)
}

// 获取agent状态
var _ = Register(AgentService.GetState)

type GetState struct {
}
type GetStateResp struct {
	IPFSState string `json:"ipfsState"`
}

func NewGetState() GetState {
	return GetState{}
}
func NewGetStateResp(ipfsState string) GetStateResp {
	return GetStateResp{
		IPFSState: ipfsState,
	}
}
func (client *Client) GetState(msg GetState, opts ...mq.RequestOption) (*GetStateResp, error) {
	return mq.Request[GetStateResp](client.rabbitCli, msg, opts...)
}
