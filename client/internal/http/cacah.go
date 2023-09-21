package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	stgsdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type CacheService struct {
	*Server
}

func (s *Server) CacheSvc() *CacheService {
	return &CacheService{
		Server: s,
	}
}

type CacheMovePackageReq struct {
	UserID    *int64 `json:"userID" binding:"required"`
	PackageID *int64 `json:"packageID" binding:"required"`
	NodeID    *int64 `json:"nodeID" binding:"required"`
}
type CacheMovePackageResp struct {
	CacheInfos []stgsdk.ObjectCacheInfo `json:"cacheInfos"`
}

func (s *CacheService) MovePackage(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Cache.LoadPackage")

	var req CacheMovePackageReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	taskID, err := s.svc.CacheSvc().StartCacheMovePackage(*req.UserID, *req.PackageID, *req.NodeID)
	if err != nil {
		log.Warnf("start cache move package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "cache move package failed"))
		return
	}

	for {
		complete, cacheInfos, err := s.svc.CacheSvc().WaitCacheMovePackage(*req.NodeID, taskID, time.Second*10)
		if complete {
			if err != nil {
				log.Warnf("moving complete with: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "cache move package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(CacheMovePackageResp{
				CacheInfos: cacheInfos,
			}))
			return
		}

		if err != nil {
			log.Warnf("wait moving: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "cache move package failed"))
			return
		}
	}
}
