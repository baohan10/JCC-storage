package http

import (
	"mime/multipart"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
)

type PackageService struct {
	*Server
}

func (s *Server) PackageSvc() *PackageService {
	return &PackageService{
		Server: s,
	}
}

type PackageUploadReq struct {
	Info  PackageUploadInfo       `form:"info" binding:"required"`
	Files []*multipart.FileHeader `form:"files"`
}

type PackageUploadInfo struct {
	UserID     *int64                     `json:"userID" binding:"required"`
	BucketID   *int64                     `json:"bucketID" binding:"required"`
	Name       string                     `json:"name" binding:"required"`
	Redundancy models.TypedRedundancyInfo `json:"redundancy" binding:"required"`
}

type PackageUploadResp struct {
	PackageID int64 `json:"packageID,string"`
}

func (s *PackageService) Upload(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.Upload")

	var req PackageUploadReq
	if err := ctx.ShouldBind(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	switch req.Info.Redundancy.Type {
	case models.RedundancyRep:
		s.uploadRep(ctx, &req)
		return
	case models.RedundancyEC:
		s.uploadEC(ctx, &req)
		return
	}

	ctx.JSON(http.StatusForbidden, Failed(errorcode.OperationFailed, "not supported yet"))
}

func (s *PackageService) uploadRep(ctx *gin.Context, req *PackageUploadReq) {
	log := logger.WithField("HTTP", "Package.Upload")

	var repInfo models.RepRedundancyInfo
	if err := serder.AnyToAny(req.Info.Redundancy.Info, &repInfo); err != nil {
		log.Warnf("parsing rep redundancy config: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "invalid rep redundancy config"))
		return
	}

	objIter := iterator.NewHTTPObjectIterator(req.Files)

	taskID, err := s.svc.PackageSvc().StartCreatingRepPackage(*req.Info.UserID, *req.Info.BucketID, req.Info.Name, objIter, repInfo)

	if err != nil {
		log.Warnf("start uploading rep package task: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "start uploading task failed"))
		return
	}

	for {
		complete, createResult, err := s.svc.PackageSvc().WaitCreatingRepPackage(taskID, time.Second*5)
		if complete {
			if err != nil {
				log.Warnf("uploading rep package: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "uploading rep package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(PackageUploadResp{
				PackageID: createResult.PackageID,
			}))
			return
		}

		if err != nil {
			log.Warnf("waiting task: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "wait uploading task failed"))
			return
		}
	}
}

func (s *PackageService) uploadEC(ctx *gin.Context, req *PackageUploadReq) {
	log := logger.WithField("HTTP", "Package.Upload")

	var ecInfo models.ECRedundancyInfo
	if err := serder.AnyToAny(req.Info.Redundancy.Info, &ecInfo); err != nil {
		log.Warnf("parsing ec redundancy config: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "invalid rep redundancy config"))
		return
	}

	objIter := iterator.NewHTTPObjectIterator(req.Files)

	taskID, err := s.svc.PackageSvc().StartCreatingECPackage(*req.Info.UserID, *req.Info.BucketID, req.Info.Name, objIter, ecInfo)

	if err != nil {
		log.Warnf("start uploading ec package task: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "start uploading task failed"))
		return
	}

	for {
		complete, createResult, err := s.svc.PackageSvc().WaitCreatingECPackage(taskID, time.Second*5)
		if complete {
			if err != nil {
				log.Warnf("uploading ec package: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "uploading ec package failed"))
				return
			}

			ctx.JSON(http.StatusOK, OK(PackageUploadResp{
				PackageID: createResult.PackageID,
			}))
			return
		}

		if err != nil {
			log.Warnf("waiting task: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "wait uploading task failed"))
			return
		}
	}
}

type PackageDeleteReq struct {
	UserID    *int64 `json:"userID" binding:"required"`
	PackageID *int64 `json:"packageID" binding:"required"`
}

func (s *PackageService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.Delete")

	var req PackageDeleteReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	err := s.svc.PackageSvc().DeletePackage(*req.UserID, *req.PackageID)
	if err != nil {
		log.Warnf("deleting package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "delete package failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
}

type PackageGetCacheNodeIDs struct {
	UserID    *int64 `json:"userID" binding:"required"`
	PackageID *int64 `json:"packageID" binding:"required"`
}
type GetCacheNodesByPackageResp struct {
	NodeIDs       []int64 `json:"nodeIDs"`
	RedunancyType string  `json:"redunancyType,string"`
}

func (s *PackageService) GetCacheNodeIDs(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.GetCacheNodeIDs")

	var req PackageGetCacheNodeIDs
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodeIDs, redunancyType, err := s.svc.PackageSvc().GetCacheNodesByPackage(*req.UserID, *req.PackageID)
	if err != nil {
		log.Warnf("get cache nodes by packageID failed: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get cache nodes by packageID failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(GetCacheNodesByPackageResp{
		NodeIDs:       nodeIDs,
		RedunancyType: redunancyType,
	}))
}

type PackageGetStorageNodeIDs struct {
	UserID    *int64 `json:"userID" binding:"required"`
	PackageID *int64 `json:"packageID" binding:"required"`
}

type GetStorageNodesByPackageResp struct {
	NodeIDs []int64 `json:"nodeIDs"`
}

func (s *PackageService) GetStorageNodeIDs(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.GetStorageNodeIDs")

	var req PackageGetStorageNodeIDs
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodeIDs, err := s.svc.PackageSvc().GetStorageNodesByPackage(*req.UserID, *req.PackageID)
	if err != nil {
		log.Warnf("get storage nodes by packageID failed: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get storage nodes by packageID failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(GetStorageNodesByPackageResp{
		NodeIDs: nodeIDs,
	}))
}
