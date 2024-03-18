package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type StorageService struct {
	*Server
}

func (s *Server) Storage() *StorageService {
	return &StorageService{
		Server: s,
	}
}

type StorageLoadPackageReq struct {
	UserID    *cdssdk.UserID    `json:"userID" binding:"required"`
	PackageID *cdssdk.PackageID `json:"packageID" binding:"required"`
	StorageID *cdssdk.StorageID `json:"storageID" binding:"required"`
}

type StorageLoadPackageResp struct {
	cdssdk.StorageLoadPackageResp
}

func (s *StorageService) LoadPackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.LoadPackage")

	var req StorageLoadPackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodeID, taskID, err := s.svc.StorageSvc().StartStorageLoadPackage(*req.UserID, *req.PackageID, *req.StorageID)
	if err != nil {
		log.Warnf("start storage load package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage load package failed"))
		return
	}

	for {
		complete, fullPath, err := s.svc.StorageSvc().WaitStorageLoadPackage(nodeID, taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("loading complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage load package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(StorageLoadPackageResp{
				StorageLoadPackageResp: cdssdk.StorageLoadPackageResp{
					FullPath: fullPath,
				},
			}))
			return
		}

		if err != nil {
			log.Warnf("wait loadding: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage load package failed"))
			return
		}
	}
}

type StorageCreatePackageReq struct {
	UserID       *cdssdk.UserID    `json:"userID" binding:"required"`
	StorageID    *cdssdk.StorageID `json:"storageID" binding:"required"`
	Path         string            `json:"path" binding:"required"`
	BucketID     *cdssdk.BucketID  `json:"bucketID" binding:"required"`
	Name         string            `json:"name" binding:"required"`
	NodeAffinity *cdssdk.NodeID    `json:"nodeAffinity"`
}

type StorageCreatePackageResp struct {
	PackageID cdssdk.PackageID `json:"packageID"`
}

func (s *StorageService) CreatePackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.CreatePackage")

	var req StorageCreatePackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodeID, taskID, err := s.svc.StorageSvc().StartStorageCreatePackage(
		*req.UserID, *req.BucketID, req.Name, *req.StorageID, req.Path, req.NodeAffinity)
	if err != nil {
		log.Warnf("start storage create package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage create package failed"))
		return
	}

	for {
		complete, packageID, err := s.svc.StorageSvc().WaitStorageCreatePackage(nodeID, taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("creating complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage create package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(StorageCreatePackageResp{
				PackageID: packageID,
			}))
			return
		}

		if err != nil {
			log.Warnf("wait creating: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage create package failed"))
			return
		}
	}
}

type StorageGetInfoReq struct {
	UserID    *cdssdk.UserID    `form:"userID" binding:"required"`
	StorageID *cdssdk.StorageID `form:"storageID" binding:"required"`
}

type StorageGetInfoResp struct {
	cdssdk.StorageGetInfoResp
}

func (s *StorageService) GetInfo(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.GetInfo")

	var req StorageGetInfoReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	info, err := s.svc.StorageSvc().GetInfo(*req.UserID, *req.StorageID)
	if err != nil {
		log.Warnf("getting info: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get storage inf failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(StorageGetInfoResp{
		StorageGetInfoResp: cdssdk.StorageGetInfoResp{
			Name:      info.Name,
			NodeID:    info.NodeID,
			Directory: info.Directory,
		},
	}))
}
