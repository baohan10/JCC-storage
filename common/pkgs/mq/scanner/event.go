package scanner

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type EventService interface {
	PostEvent(event *PostEvent)
}

// 投递Event
var _ = RegisterNoReply(Service.PostEvent)

type PostEvent struct {
	mq.MessageBodyBase
	Event       scevt.Event `json:"event"`
	IsEmergency bool        `json:"isEmergency"` // 重要消息，优先处理
	DontMerge   bool        `json:"dontMerge"`   // 不可合并此消息
}

func NewPostEvent(event scevt.Event, isEmergency bool, dontMerge bool) *PostEvent {
	return &PostEvent{
		Event:       event,
		IsEmergency: isEmergency,
		DontMerge:   dontMerge,
	}
}
func (client *Client) PostEvent(msg *PostEvent) error {
	return mq.Send(Service.PostEvent, client.rabbitCli, msg)
}

func init() {
	mq.RegisterUnionType(scevt.EventTypeUnino)
}
