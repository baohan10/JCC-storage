package services

import (
	"fmt"

	"gitlink.org.cn/cloudream/storage/common/globals"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type ScannerService struct {
	*Service
}

func (svc *Service) ScannerSvc() *ScannerService {
	return &ScannerService{Service: svc}
}

func (svc *ScannerService) PostEvent(event scevt.Event, isEmergency bool, dontMerge bool) error {
	scCli, err := globals.ScannerMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new scacnner client: %w", err)
	}
	defer scCli.Close()

	err = scCli.PostEvent(scmq.NewPostEvent(event, isEmergency, dontMerge))
	if err != nil {
		return fmt.Errorf("request to scanner failed, err: %w", err)
	}

	return nil
}
