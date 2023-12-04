package services

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	mytask "gitlink.org.cn/cloudream/storage/client/internal/task"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtcmd "gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type PackageService struct {
	*Service
}

func (svc *Service) PackageSvc() *PackageService {
	return &PackageService{Service: svc}
}

func (svc *PackageService) Get(userID cdssdk.UserID, packageID cdssdk.PackageID) (*model.Package, error) {
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

func (svc *PackageService) DownloadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID) (iterator.DownloadingObjectIterator, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	mutex, err := reqbuilder.NewBuilder().
		// 用于判断用户是否有对象权限
		Metadata().UserBucket().ReadAny().
		// 用于查询可用的下载节点
		Node().ReadAny().
		// 用于读取包信息
		Package().ReadOne(packageID).
		// 用于读取包内的文件信息
		Object().ReadAny().
		// 用于查询Rep配置
		ObjectRep().ReadAny().
		// 用于查询Block配置
		ObjectBlock().ReadAny().
		// 用于查询包含了副本的节点
		Cache().ReadAny().
		MutexLock(svc.DistLock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}

	getObjsResp, err := coorCli.GetPackageObjectDetails(coormq.NewGetPackageObjectDetails(packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package object details: %w", err)
	}

	iter := iterator.NewObjectIterator(getObjsResp.Objects, &iterator.DownloadContext{
		Distlock: svc.DistLock,
	})

	iter.OnClosing = func() {
		mutex.Unlock()
	}
	return iter, nil
}

func (svc *PackageService) StartCreatingPackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, objIter iterator.UploadingObjectIterator, nodeAffinity *cdssdk.NodeID) (string, error) {
	tsk := svc.TaskMgr.StartNew(mytask.NewCreatePackage(userID, bucketID, name, objIter, nodeAffinity))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitCreatingPackage(taskID string, waitTimeout time.Duration) (bool, *agtcmd.CreatePackageResult, error) {
	tsk := svc.TaskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		cteatePkgTask := tsk.Body().(*mytask.CreatePackage)
		return true, cteatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) StartUpdatingPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, objIter iterator.UploadingObjectIterator) (string, error) {
	tsk := svc.TaskMgr.StartNew(mytask.NewUpdatePackage(userID, packageID, objIter))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitUpdatingPackage(taskID string, waitTimeout time.Duration) (bool, *agtcmd.UpdatePackageResult, error) {
	tsk := svc.TaskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		updatePkgTask := tsk.Body().(*mytask.UpdatePackage)
		return true, updatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) DeletePackage(userID cdssdk.UserID, packageID cdssdk.PackageID) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于判断用户是否有对象的权限
		UserBucket().ReadAny().
		// 用于读取、修改包信息
		Package().WriteOne(packageID).
		// 用于删除包内的所有文件
		Object().WriteAny().
		// 用于删除Rep配置
		ObjectRep().WriteAny().
		// 用于删除Block配置
		ObjectBlock().WriteAny().
		// 用于修改Move此Object的记录的状态
		StoragePackage().WriteAny().
		MutexLock(svc.DistLock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

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
		NodeInfos:     resp.NodeInfos,
		PackageSize:   resp.PackageSize,
		RedunancyType: resp.RedunancyType,
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
