package services

import (
	"fmt"
	"io"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type ObjectService struct {
	*Service
}

func (svc *Service) ObjectSvc() *ObjectService {
	return &ObjectService{Service: svc}
}

func (svc *ObjectService) Download(userID cdssdk.UserID, objectID cdssdk.ObjectID) (io.ReadCloser, error) {
	panic("not implement yet!")
}

func (svc *ObjectService) GetPackageObjects(userID cdssdk.UserID, packageID cdssdk.PackageID) ([]model.Object, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetPackageObjects(coormq.NewGetPackageObjects(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return getResp.Objects, nil
}
