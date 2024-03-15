package agent

import "gitlink.org.cn/cloudream/common/pkgs/mq"

type ObjectService interface {
	PinObject(msg *PinObject) (*PinObjectResp, *mq.CodeMessage)
}

// 启动Pin对象的任务
var _ = Register(Service.PinObject)

type PinObject struct {
	mq.MessageBodyBase
	FileHashes   []string `json:"fileHashes"`
	IsBackground bool     `json:"isBackground"`
}
type PinObjectResp struct {
	mq.MessageBodyBase
}

func ReqPinObject(fileHashes []string, isBackground bool) *PinObject {
	return &PinObject{
		FileHashes:   fileHashes,
		IsBackground: isBackground,
	}
}
func RespPinObject() *PinObjectResp {
	return &PinObjectResp{}
}
func (client *Client) PinObject(msg *PinObject, opts ...mq.RequestOption) (*PinObjectResp, error) {
	return mq.Request(Service.PinObject, client.rabbitCli, msg, opts...)
}
