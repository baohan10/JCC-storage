package services

import (
	"fmt"
)

type ScannerService struct {
	*Service
}

func ScannerSvc(svc *Service) *ScannerService {
	return &ScannerService{Service: svc}
}

func (svc *ScannerService) PostEvent(event any, isEmergency bool, dontMerge bool) error {
	err := svc.scanner.PostEvent(event, isEmergency, dontMerge)
	if err != nil {
		return fmt.Errorf("request to scanner failed, err: %w", err)
	}

	return nil
}
