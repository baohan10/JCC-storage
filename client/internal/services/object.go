package services

import "io"

type ObjectService struct {
	*Service
}

func (svc *Service) ObjectSvc() *ObjectService {
	return &ObjectService{Service: svc}
}

func (svc *ObjectService) Download(userID int64, objectID int64) (io.ReadCloser, error) {
	panic("not implement yet!")
}
