package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

type DownloadPackage struct {
	userID     int64
	packageID  int64
	outputPath string
}

func NewDownloadPackage(userID int64, packageID int64, outputPath string) *DownloadPackage {
	return &DownloadPackage{
		userID:     userID,
		packageID:  packageID,
		outputPath: outputPath,
	}
}

func (t *DownloadPackage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *DownloadPackage) do(ctx TaskContext) error {
	getPkgResp, err := ctx.Coordinator().GetPackage(coormq.NewGetPackage(t.userID, t.packageID))
	if err != nil {

		return fmt.Errorf("getting package: %w", err)
	}

	var objIter iterator.DownloadingObjectIterator
	if getPkgResp.Redundancy.Type == models.RedundancyRep {
		objIter, err = t.downloadRep(ctx)
	} else {
		objIter, err = t.downloadEC(ctx, getPkgResp.Package)
	}
	if err != nil {
		return err
	}
	defer objIter.Close()

	return t.writeObject(objIter)
}

func (t *DownloadPackage) downloadRep(ctx TaskContext) (iterator.DownloadingObjectIterator, error) {
	getObjsResp, err := ctx.Coordinator().GetPackageObjects(coormq.NewGetPackageObjects(t.userID, t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package objects: %w", err)
	}

	getObjRepDataResp, err := ctx.Coordinator().GetPackageObjectRepData(coormq.NewGetPackageObjectRepData(t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package object rep data: %w", err)
	}

	iter := iterator.NewRepObjectIterator(getObjsResp.Objects, getObjRepDataResp.Data, ctx.Coordinator(), svc.distlock, myos.DownloadConfig{
		LocalIPFS:  svc.ipfs,
		ExternalIP: config.Cfg().ExternalIP,
		GRPCPort:   config.Cfg().GRPCPort,
		MQ:         &config.Cfg().RabbitMQ,
	})

	return iter, nil
}

func (t *DownloadPackage) downloadEC(ctx TaskContext, pkg model.Package) (iterator.DownloadingObjectIterator, error) {
	getObjsResp, err := ctx.Coordinator().GetPackageObjects(coormq.NewGetPackageObjects(t.userID, t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package objects: %w", err)
	}

	getObjECDataResp, err := ctx.Coordinator().GetPackageObjectECData(coormq.NewGetPackageObjectECData(t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package object ec data: %w", err)
	}

	var ecRed models.ECRedundancyInfo
	if err := serder.AnyToAny(pkg.Redundancy.Info, &ecRed); err != nil {
		return nil, fmt.Errorf("get ec redundancy info: %w", err)
	}

	getECResp, err := ctx.Coordinator().GetECConfig(coormq.NewGetECConfig(ecRed.ECName))
	if err != nil {
		return nil, fmt.Errorf("getting ec: %w", err)
	}

	iter := iterator.NewECObjectIterator(getObjsResp.Objects, getObjECDataResp.Data, ctx.Coordinator(), svc.distlock, getECResp.Config, config.Cfg().ECPacketSize, myos.DownloadConfig{
		LocalIPFS:  svc.ipfs,
		ExternalIP: config.Cfg().ExternalIP,
		GRPCPort:   config.Cfg().GRPCPort,
		MQ:         &config.Cfg().RabbitMQ,
	})

	return iter, nil
}

func (t *DownloadPackage) writeObject(objIter iterator.DownloadingObjectIterator) error {
	for {
		objInfo, err := objIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break
		}
		if err != nil {
			return err
		}
		defer objInfo.File.Close()

		outputFile, err := os.Create(filepath.Join(t.outputPath, objInfo.Object.Path))
		if err != nil {
			return fmt.Errorf("creating object file: %w", err)
		}
		defer outputFile.Close()

		_, err = io.Copy(outputFile, objInfo.File)
		if err != nil {
			return fmt.Errorf("copy object data to local file failed, err: %w", err)
		}
	}

	return nil
}
