package mq

import (
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
	"gitlink.org.cn/cloudream/storage/scanner/internal/event"
)

func (svc *Service) PostEvent(msg *scmq.PostEvent) {
	evt, err := event.FromMessage(msg.Event)
	if err != nil {
		logger.Warnf("create event from event message failed, err: %s", err.Error())
		return
	}

	svc.eventExecutor.Post(evt, event.ExecuteOption{
		IsEmergency: msg.IsEmergency,
		DontMerge:   msg.DontMerge,
	})
}
