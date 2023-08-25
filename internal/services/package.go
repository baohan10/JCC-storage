package services

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/storage-client/internal/config"
	mytask "gitlink.org.cn/cloudream/storage-client/internal/task"
	"gitlink.org.cn/cloudream/storage-common/globals"
	agtcmd "gitlink.org.cn/cloudream/storage-common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage-common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

type PackageService struct {
	*Service
}

func (svc *Service) PackageSvc() *PackageService {
	return &PackageService{Service: svc}
}

func (svc *PackageService) DownloadPackage(userID int64, packageID int64) (iterator.DownloadingObjectIterator, error) {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

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

	getPkgResp, err := coorCli.GetPackage(coormq.NewGetPackage(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package: %w", err)
	}

	getObjsResp, err := coorCli.GetPackageObjects(coormq.NewGetPackageObjects(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package objects: %w", err)
	}

	if getPkgResp.Redundancy.Type == models.RedundancyRep {
		iter, err := svc.downloadRepPackage(packageID, getObjsResp.Objects, coorCli)

		if err != nil {
			mutex.Unlock()
			return nil, err
		}

		iter.OnClosing = func() {
			mutex.Unlock()
		}

		return iter, nil
	} else {
		iter, err := svc.downloadECPackage(getPkgResp.Package, getObjsResp.Objects, coorCli)

		if err != nil {
			mutex.Unlock()
			return nil, err
		}

		iter.OnClosing = func() {
			mutex.Unlock()
		}

		return iter, nil
	}
}

func (svc *PackageService) downloadRepPackage(packageID int64, objects []model.Object, coorCli *coormq.PoolClient) (*iterator.RepObjectIterator, error) {
	getObjRepDataResp, err := coorCli.GetPackageObjectRepData(coormq.NewGetPackageObjectRepData(packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package object rep data: %w", err)
	}

	iter := iterator.NewRepObjectIterator(objects, getObjRepDataResp.Data, &iterator.DownloadContext{
		Distlock: svc.DistLock,
	})

	return iter, nil
}
func (svc *PackageService) downloadECPackage(pkg model.Package, objects []model.Object, coorCli *coormq.PoolClient) (*iterator.ECObjectIterator, error) {
	getObjECDataResp, err := coorCli.GetPackageObjectECData(coormq.NewGetPackageObjectECData(pkg.PackageID))
	if err != nil {
		return nil, fmt.Errorf("getting package object ec data: %w", err)
	}

	var ecRed models.ECRedundancyInfo
	if err := serder.AnyToAny(pkg.Redundancy.Info, &ecRed); err != nil {
		return nil, fmt.Errorf("get ec redundancy info: %w", err)
	}

	getECResp, err := coorCli.GetECConfig(coormq.NewGetECConfig(ecRed.ECName))
	if err != nil {
		return nil, fmt.Errorf("getting ec: %w", err)
	}

	iter := iterator.NewECObjectIterator(objects, getObjECDataResp.Data, getECResp.Config, &iterator.ECDownloadContext{
		DownloadContext: &iterator.DownloadContext{
			Distlock: svc.DistLock,
		},
		ECPacketSize: config.Cfg().ECPacketSize,
	})

	return iter, nil
}

func (svc *PackageService) StartCreatingRepPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, repInfo models.RepRedundancyInfo) (string, error) {
	tsk := svc.TaskMgr.StartNew(mytask.NewCreateRepPackage(userID, bucketID, name, objIter, repInfo))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitCreatingRepPackage(taskID string, waitTimeout time.Duration) (bool, *mytask.CreateRepPackageResult, error) {
	tsk := svc.TaskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		cteatePkgTask := tsk.Body().(*mytask.CreateRepPackage)
		return true, cteatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) StartUpdatingRepPackage(userID int64, packageID int64, objIter iterator.UploadingObjectIterator) (string, error) {
	tsk := svc.TaskMgr.StartNew(mytask.NewUpdateRepPackage(userID, packageID, objIter))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitUpdatingRepPackage(taskID string, waitTimeout time.Duration) (bool, *agtcmd.UpdateRepPackageResult, error) {
	tsk := svc.TaskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		updatePkgTask := tsk.Body().(*mytask.UpdateRepPackage)
		return true, updatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) StartCreatingECPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, ecInfo models.ECRedundancyInfo) (string, error) {
	tsk := svc.TaskMgr.StartNew(mytask.NewCreateECPackage(userID, bucketID, name, objIter, ecInfo))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitCreatingECPackage(taskID string, waitTimeout time.Duration) (bool, *agtcmd.CreateRepPackageResult, error) {
	tsk := svc.TaskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		cteatePkgTask := tsk.Body().(*mytask.CreateRepPackage)
		return true, cteatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) StartUpdatingECPackage(userID int64, packageID int64, objIter iterator.UploadingObjectIterator) (string, error) {
	tsk := svc.TaskMgr.StartNew(mytask.NewUpdateECPackage(userID, packageID, objIter))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitUpdatingECPackage(taskID string, waitTimeout time.Duration) (bool, *agtcmd.UpdateECPackageResult, error) {
	tsk := svc.TaskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		updatePkgTask := tsk.Body().(*mytask.UpdateECPackage)
		return true, updatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) DeletePackage(userID int64, packageID int64) error {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

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

func (svc *PackageService) GetCachedNodes(userID int64, packageID int64) (models.PackageCachingInfo, error) {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return models.PackageCachingInfo{}, fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

	resp, err := coorCli.GetPackageCachedNodes(coormq.NewGetPackageCachedNodes(userID, packageID))
	if err != nil {
		return models.PackageCachingInfo{}, fmt.Errorf("get package cached nodes: %w", err)
	}

	tmp := models.PackageCachingInfo{
		NodeInfos:     resp.NodeInfos,
		PackageSize:   resp.PackageSize,
		RedunancyType: resp.RedunancyType,
	}
	return tmp, nil
}

func (svc *PackageService) GetLoadedNodes(userID int64, packageID int64) ([]int64, error) {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

	resp, err := coorCli.GetPackageLoadedNodes(coormq.NewGetPackageLoadedNodes(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("get package loaded nodes: %w", err)
	}
	return resp.NodeIDs, nil
}
