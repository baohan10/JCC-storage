package services

import (
	"gitlink.org.cn/cloudream/scanner/internal/event"
)

type Service struct {
	eventExecutor *event.Executor
}

func NewService(eventExecutor *event.Executor) *Service {
	return &Service{
		eventExecutor: eventExecutor,
	}
}
