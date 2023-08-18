package services

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/storage-client/internal/config"
	mytask "gitlink.org.cn/cloudream/storage-client/internal/task"
	agtcmd "gitlink.org.cn/cloudream/storage-common/pkgs/cmd"
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
	/*
		TODO2
		// TODO zkx 需要梳理EC锁涉及的锁，补充下面漏掉的部分
		mutex, err := reqbuilder.NewBuilder().
			// 用于判断用户是否有对象权限
			Metadata().UserBucket().ReadAny().
			// 用于查询可用的下载节点
			Node().ReadAny().
			// 用于读取文件信息
			Object().ReadOne(objectID).
			// 用于查询Rep配置
			ObjectRep().ReadOne(objectID).
			// 用于查询Block配置
			ObjectBlock().ReadAny().
			// 用于查询包含了副本的节点
			Cache().ReadAny().
			MutexLock(svc.distlock)
		if err != nil {
			return nil, fmt.Errorf("acquire locks failed, err: %w", err)
		}
	*/
	getPkgResp, err := svc.coordinator.GetPackage(coormq.NewGetPackage(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package: %w", err)
	}

	getObjsResp, err := svc.coordinator.GetPackageObjects(coormq.NewGetPackageObjects(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package objects: %w", err)
	}

	if getPkgResp.Redundancy.Type == models.RedundancyRep {
		getObjRepDataResp, err := svc.coordinator.GetPackageObjectRepData(coormq.NewGetPackageObjectRepData(packageID))
		if err != nil {
			return nil, fmt.Errorf("getting package object rep data: %w", err)
		}

		iter := iterator.NewRepObjectIterator(getObjsResp.Objects, getObjRepDataResp.Data, svc.coordinator, svc.distlock, iterator.DownloadConfig{
			LocalIPFS:  svc.ipfs,
			ExternalIP: config.Cfg().ExternalIP,
			GRPCPort:   config.Cfg().GRPCPort,
			MQ:         &config.Cfg().RabbitMQ,
		})

		return iter, nil
	}

	getObjECDataResp, err := svc.coordinator.GetPackageObjectECData(coormq.NewGetPackageObjectECData(packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package object ec data: %w", err)
	}

	var ecRed models.ECRedundancyInfo
	if err := serder.AnyToAny(getPkgResp.Package.Redundancy.Info, &ecRed); err != nil {
		return nil, fmt.Errorf("get ec redundancy info: %w", err)
	}

	getECResp, err := svc.coordinator.GetECConfig(coormq.NewGetECConfig(ecRed.ECName))
	if err != nil {
		return nil, fmt.Errorf("getting ec: %w", err)
	}

	iter := iterator.NewECObjectIterator(getObjsResp.Objects, getObjECDataResp.Data, svc.coordinator, svc.distlock, getECResp.Config, config.Cfg().ECPacketSize, iterator.DownloadConfig{
		LocalIPFS:  svc.ipfs,
		ExternalIP: config.Cfg().ExternalIP,
		GRPCPort:   config.Cfg().GRPCPort,
		MQ:         &config.Cfg().RabbitMQ,
	})

	return iter, nil
}

func (svc *PackageService) StartCreatingRepPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, repInfo models.RepRedundancyInfo) (string, error) {
	tsk := svc.taskMgr.StartNew(agtcmd.Wrap[mytask.TaskContext](
		agtcmd.NewCreateRepPackage(
			userID, bucketID, name, objIter,
			repInfo,
			agtcmd.UploadConfig{
				LocalIPFS:   svc.ipfs,
				LocalNodeID: nil,
				ExternalIP:  config.Cfg().ExternalIP,
				GRPCPort:    config.Cfg().GRPCPort,
				MQ:          &config.Cfg().RabbitMQ,
			})))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitCreatingRepPackage(taskID string, waitTimeout time.Duration) (bool, *agtcmd.CreateRepPackageResult, error) {
	tsk := svc.taskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		cteatePkgTask := tsk.Body().(*agtcmd.TaskWrapper[mytask.TaskContext]).InnerTask().(*agtcmd.CreateRepPackage)
		return true, &cteatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) StartUpdatingRepPackage(userID int64, packageID int64, objIter iterator.UploadingObjectIterator) (string, error) {
	tsk := svc.taskMgr.StartNew(agtcmd.Wrap[mytask.TaskContext](
		agtcmd.NewUpdateRepPackage(
			userID, packageID, objIter,
			agtcmd.UploadConfig{
				LocalIPFS:   svc.ipfs,
				LocalNodeID: nil,
				ExternalIP:  config.Cfg().ExternalIP,
				GRPCPort:    config.Cfg().GRPCPort,
				MQ:          &config.Cfg().RabbitMQ,
			})))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitUpdatingRepPackage(taskID string, waitTimeout time.Duration) (bool, *agtcmd.UpdateRepPackageResult, error) {
	tsk := svc.taskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		updatePkgTask := tsk.Body().(*agtcmd.TaskWrapper[mytask.TaskContext]).InnerTask().(*agtcmd.UpdateRepPackage)
		return true, &updatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) StartCreatingECPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, ecInfo models.ECRedundancyInfo) (string, error) {
	tsk := svc.taskMgr.StartNew(agtcmd.Wrap[mytask.TaskContext](
		agtcmd.NewCreateECPackage(
			userID, bucketID, name, objIter,
			ecInfo,
			config.Cfg().ECPacketSize,
			agtcmd.UploadConfig{
				LocalIPFS:   svc.ipfs,
				LocalNodeID: nil,
				ExternalIP:  config.Cfg().ExternalIP,
				GRPCPort:    config.Cfg().GRPCPort,
				MQ:          &config.Cfg().RabbitMQ,
			})))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitCreatingECPackage(taskID string, waitTimeout time.Duration) (bool, *agtcmd.CreateRepPackageResult, error) {
	tsk := svc.taskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		cteatePkgTask := tsk.Body().(*agtcmd.TaskWrapper[mytask.TaskContext]).InnerTask().(*agtcmd.CreateRepPackage)
		return true, &cteatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) StartUpdatingECPackage(userID int64, packageID int64, objIter iterator.UploadingObjectIterator) (string, error) {
	tsk := svc.taskMgr.StartNew(agtcmd.Wrap[mytask.TaskContext](
		agtcmd.NewUpdateECPackage(
			userID, packageID, objIter,
			config.Cfg().ECPacketSize,
			agtcmd.UploadConfig{
				LocalIPFS:   svc.ipfs,
				LocalNodeID: nil,
				ExternalIP:  config.Cfg().ExternalIP,
				GRPCPort:    config.Cfg().GRPCPort,
				MQ:          &config.Cfg().RabbitMQ,
			})))
	return tsk.ID(), nil
}

func (svc *PackageService) WaitUpdatingECPackage(taskID string, waitTimeout time.Duration) (bool, *agtcmd.UpdateECPackageResult, error) {
	tsk := svc.taskMgr.FindByID(taskID)
	if tsk.WaitTimeout(waitTimeout) {
		updatePkgTask := tsk.Body().(*agtcmd.TaskWrapper[mytask.TaskContext]).InnerTask().(*agtcmd.UpdateECPackage)
		return true, &updatePkgTask.Result, tsk.Error()
	}
	return false, nil, nil
}

func (svc *PackageService) DeletePackage(userID int64, packageID int64) error {
	/*
		// TODO2
		mutex, err := reqbuilder.NewBuilder().
			Metadata().
			// 用于判断用户是否有对象的权限
			UserBucket().ReadAny().
			// 用于读取、修改对象信息
			Object().WriteOne(objectID).
			// 用于删除Rep配置
			ObjectRep().WriteOne(objectID).
			// 用于删除Block配置
			ObjectBlock().WriteAny().
			// 用于修改Move此Object的记录的状态
			StorageObject().WriteAny().
			MutexLock(svc.distlock)
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()
	*/

	_, err := svc.coordinator.DeletePackage(coormq.NewDeletePackage(userID, packageID))
	if err != nil {
		return fmt.Errorf("deleting package: %w", err)
	}

	return nil
}
