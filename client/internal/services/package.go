package services

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type PackageService struct {
	*Service
}

func (svc *Service) PackageSvc() *PackageService {
	return &PackageService{Service: svc}
}

func (svc *PackageService) Get(userID cdssdk.UserID, packageID cdssdk.PackageID) (*cdssdk.Package, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetPackage(coormq.NewGetPackage(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return &getResp.Package, nil
}

func (svc *PackageService) GetByName(userID cdssdk.UserID, bucketName string, packageName string) (*cdssdk.Package, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetPackageByName(coormq.ReqGetPackageByName(userID, bucketName, packageName))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return &getResp.Package, nil
}

func (svc *PackageService) GetBucketPackages(userID cdssdk.UserID, bucketID cdssdk.BucketID) ([]cdssdk.Package, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetBucketPackages(coormq.NewGetBucketPackages(userID, bucketID))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return getResp.Packages, nil
}

func (svc *PackageService) Create(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string) (cdssdk.Package, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return cdssdk.Package{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.CreatePackage(coormq.NewCreatePackage(userID, bucketID, name))
	if err != nil {
		return cdssdk.Package{}, fmt.Errorf("creating package: %w", err)
	}

	return resp.Package, nil
}

func (svc *PackageService) DownloadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID) (iterator.DownloadingObjectIterator, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getObjsResp, err := coorCli.GetPackageObjectDetails(coormq.NewGetPackageObjectDetails(packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package object details: %w", err)
	}

	iter := iterator.NewDownloadObjectIterator(getObjsResp.Objects, &iterator.DownloadContext{
		Distlock: svc.DistLock,
	})

	return iter, nil
}

func (svc *PackageService) DeletePackage(userID cdssdk.UserID, packageID cdssdk.PackageID) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	_, err = coorCli.DeletePackage(coormq.NewDeletePackage(userID, packageID))
	if err != nil {
		return fmt.Errorf("deleting package: %w", err)
	}

	return nil
}

func (svc *PackageService) GetCachedNodes(userID cdssdk.UserID, packageID cdssdk.PackageID) (cdssdk.PackageCachingInfo, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return cdssdk.PackageCachingInfo{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.GetPackageCachedNodes(coormq.NewGetPackageCachedNodes(userID, packageID))
	if err != nil {
		return cdssdk.PackageCachingInfo{}, fmt.Errorf("get package cached nodes: %w", err)
	}

	tmp := cdssdk.PackageCachingInfo{
		NodeInfos:   resp.NodeInfos,
		PackageSize: resp.PackageSize,
	}
	return tmp, nil
}

func (svc *PackageService) GetLoadedNodes(userID cdssdk.UserID, packageID cdssdk.PackageID) ([]cdssdk.NodeID, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	resp, err := coorCli.GetPackageLoadedNodes(coormq.NewGetPackageLoadedNodes(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("get package loaded nodes: %w", err)
	}
	return resp.NodeIDs, nil
}
