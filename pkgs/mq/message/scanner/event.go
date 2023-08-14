package scanner

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkg/mq"
	scevt "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/scanner/event"
)

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

func init() {
	mq.RegisterMessage[PostEvent]()
}
