package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlink.org.cn/cloudream/common/models"
	distsvc "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type DownloadPackage struct {
	userID     int64
	packageID  int64
	outputPath string
}

type DownloadPackageContext struct {
	Distlock *distsvc.Service
}

func NewDownloadPackage(userID int64, packageID int64, outputPath string) *DownloadPackage {
	return &DownloadPackage{
		userID:     userID,
		packageID:  packageID,
		outputPath: outputPath,
	}
}

func (t *DownloadPackage) Execute(ctx *DownloadPackageContext) error {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

	getPkgResp, err := coorCli.GetPackage(coormq.NewGetPackage(t.userID, t.packageID))
	if err != nil {

		return fmt.Errorf("getting package: %w", err)
	}

	var objIter iterator.DownloadingObjectIterator
	if getPkgResp.Redundancy.IsRepInfo() {
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

func (t *DownloadPackage) downloadRep(ctx *DownloadPackageContext) (iterator.DownloadingObjectIterator, error) {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

	getObjsResp, err := coorCli.GetPackageObjects(coormq.NewGetPackageObjects(t.userID, t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package objects: %w", err)
	}

	getObjRepDataResp, err := coorCli.GetPackageObjectRepData(coormq.NewGetPackageObjectRepData(t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package object rep data: %w", err)
	}

	iter := iterator.NewRepObjectIterator(getObjsResp.Objects, getObjRepDataResp.Data, &iterator.DownloadContext{
		Distlock: ctx.Distlock,
	})

	return iter, nil
}

func (t *DownloadPackage) downloadEC(ctx *DownloadPackageContext, pkg model.Package) (iterator.DownloadingObjectIterator, error) {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

	getObjsResp, err := coorCli.GetPackageObjects(coormq.NewGetPackageObjects(t.userID, t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package objects: %w", err)
	}

	getObjECDataResp, err := coorCli.GetPackageObjectECData(coormq.NewGetPackageObjectECData(t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package object ec data: %w", err)
	}

	var ecInfo models.ECRedundancyInfo
	if ecInfo, err = pkg.Redundancy.ToECInfo(); err != nil {
		return nil, fmt.Errorf("get ec redundancy info: %w", err)
	}

	getECResp, err := coorCli.GetECConfig(coormq.NewGetECConfig(ecInfo.ECName))
	if err != nil {
		return nil, fmt.Errorf("getting ec: %w", err)
	}

	iter := iterator.NewECObjectIterator(getObjsResp.Objects, getObjECDataResp.Data, ecInfo, getECResp.Config, &iterator.DownloadContext{
		Distlock: ctx.Distlock,
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

		err = func() error {
			defer objInfo.File.Close()

			fullPath := filepath.Join(t.outputPath, objInfo.Object.Path)

			dirPath := filepath.Dir(fullPath)
			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return fmt.Errorf("creating object dir: %w", err)
			}

			outputFile, err := os.Create(fullPath)
			if err != nil {
				return fmt.Errorf("creating object file: %w", err)
			}
			defer outputFile.Close()

			_, err = io.Copy(outputFile, objInfo.File)
			if err != nil {
				return fmt.Errorf("copy object data to local file failed, err: %w", err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}
