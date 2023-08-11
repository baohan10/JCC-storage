package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkg/logger"
)

type StorageService struct {
	*Server
}

func (s *Server) StorageSvc() *StorageService {
	return &StorageService{
		Server: s,
	}
}

type StorageMoveObjectReq struct {
	UserID    *int64 `json:"userID" binding:"required"`
	ObjectID  *int64 `json:"objectID" binding:"required"`
	StorageID *int64 `json:"storageID" binding:"required"`
}

func (s *StorageService) MoveObject(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Storage.MoveObject")

	var req StorageMoveObjectReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	taskID, err := s.svc.StorageSvc().StartStorageMoveObject(*req.UserID, *req.ObjectID, *req.StorageID)
	if err != nil {
		log.Warnf("start storage move object: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage move object failed"))
		return
	}

	for {
		complete, err := s.svc.StorageSvc().WaitStorageMoveObject(taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("moving complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage move object failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(nil))
			return
		}

		if err != nil {
			log.Warnf("wait moving: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "storage move object failed"))
			return
		}
	}
}
