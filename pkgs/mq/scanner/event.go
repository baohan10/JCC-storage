package scanner

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/mq"
	scevt "gitlink.org.cn/cloudream/storage-common/pkgs/mq/scanner/event"
)

type EventService interface {
	PostEvent(event *PostEvent)
}

// 投递Event
var _ = RegisterNoReply(EventService.PostEvent)

type PostEvent struct {
	Event       map[string]any `json:"event"`
	IsEmergency bool           `json:"isEmergency"` // 重要消息，优先处理
	DontMerge   bool           `json:"dontMerge"`   // 不可合并此消息
}

func NewPostEvent(event any, isEmergency bool, dontMerge bool) (PostEvent, error) {
	mp, err := scevt.MessageToMap(event)
	if err != nil {
		return PostEvent{}, fmt.Errorf("message to map failed, err: %w", err)
	}

	return PostEvent{
		Event:       mp,
		IsEmergency: isEmergency,
		DontMerge:   dontMerge,
	}, nil
}
func (cli *Client) PostEvent(event any, isEmergency bool, dontMerge bool, opts ...mq.SendOption) error {
	opt := mq.SendOption{
		Timeout: time.Second * 30,
	}
	if len(opts) > 0 {
		opt = opts[0]
	}

	body, err := NewPostEvent(event, isEmergency, dontMerge)
	if err != nil {
		return fmt.Errorf("new post event body failed, err: %w", err)
	}

	return mq.Send(cli.rabbitCli, body, opt)
}
