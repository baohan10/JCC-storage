package cmd

import (
	"gitlink.org.cn/cloudream/agent/internal/event"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	agtevt "gitlink.org.cn/cloudream/rabbitmq/message/agent/event"
)

func (svc *Service) PostEvent(msg *agtmsg.PostEvent) {

	evtMsg, err := agtevt.MapToMessage(msg.Body.Event)
	if err != nil {
		logger.Warnf("convert map to event message failed, err: %s", err.Error())
		return
	}

	evt, err := event.FromMessage(evtMsg)
	if err != nil {
		logger.Warnf("create event from event message failed, err: %s", err.Error())
		return
	}

	svc.eventExecutor.Post(evt, event.ExecuteOption{
		IsEmergency: msg.Body.IsEmergency,
		DontMerge:   msg.Body.DontMerge,
	})
}
