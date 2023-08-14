package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
)

type GetState struct {
}

func NewGetState() GetState {
	return GetState{}
}

type GetStateResp struct {
	IPFSState string `json:"ipfsState"`
}

func NewGetStateRespBody(ipfsState string) GetStateResp {
	return GetStateResp{
		IPFSState: ipfsState,
	}
}

func init() {
	mq.RegisterMessage[GetState]()
	mq.RegisterMessage[GetStateResp]()
}
