package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
)

type StorageService struct {
	*Server
}

func (s *Server) StorageSvc() *StorageService {
	return &StorageService{
		Server: s,
	}
}

type StorageMovePackageReq struct {
	UserID    *int64 `json:"userID" binding:"required"`
	PackageID *int64 `json:"packageID" binding:"required"`
	StorageID *int64 `json:"storageID" binding:"required"`
}

func (s *StorageService) MovePackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.MovePackage")

	var req StorageMovePackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	taskID, err := s.svc.StorageSvc().StartStorageMovePackage(*req.UserID, *req.PackageID, *req.StorageID)
	if err != nil {
		log.Warnf("start storage move package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage move package failed"))
		return
	}

	for {
		complete, err := s.svc.StorageSvc().WaitStorageMovePackage(taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("moving complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage move package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(nil))
			return
		}

		if err != nil {
			log.Warnf("wait moving: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage move package failed"))
			return
		}
	}
}
