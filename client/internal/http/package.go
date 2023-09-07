package http

import (
	"mime/multipart"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/common/pkgs/logger"

	stgiter "gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
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
	UserID       *int64                     `json:"userID" binding:"required"`
	BucketID     *int64                     `json:"bucketID" binding:"required"`
	Name         string                     `json:"name" binding:"required"`
	Redundancy   models.TypedRedundancyInfo `json:"redundancy" binding:"required"`
	NodeAffinity *int64                     `json:"nodeAffinity"`
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

	if req.Info.Redundancy.IsRepInfo() {
		s.uploadRep(ctx, &req)
		return
	}

	if req.Info.Redundancy.IsECInfo() {
		s.uploadEC(ctx, &req)
		return
	}

	ctx.JSON(http.StatusForbidden, Failed(errorcode.OperationFailed, "not supported yet"))
}

func (s *PackageService) uploadRep(ctx *gin.Context, req *PackageUploadReq) {
	log := logger.WithField("HTTP", "Package.Upload")

	var err error
	var repInfo models.RepRedundancyInfo
	if repInfo, err = req.Info.Redundancy.ToRepInfo(); err != nil {
		log.Warnf("parsing rep redundancy config: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "invalid rep redundancy config"))
		return
	}

	objIter := mapMultiPartFileToUploadingObject(req.Files)

	taskID, err := s.svc.PackageSvc().StartCreatingRepPackage(*req.Info.UserID, *req.Info.BucketID, req.Info.Name, objIter, repInfo, req.Info.NodeAffinity)

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

	var err error
	var ecInfo models.ECRedundancyInfo
	if ecInfo, err = req.Info.Redundancy.ToECInfo(); err != nil {
		log.Warnf("parsing ec redundancy config: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "invalid rep redundancy config"))
		return
	}

	objIter := mapMultiPartFileToUploadingObject(req.Files)

	taskID, err := s.svc.PackageSvc().StartCreatingECPackage(*req.Info.UserID, *req.Info.BucketID, req.Info.Name, objIter, ecInfo, req.Info.NodeAffinity)

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

type GetCachedNodesReq struct {
	UserID    *int64 `json:"userID" binding:"required"`
	PackageID *int64 `json:"packageID" binding:"required"`
}
type GetCachedNodesResp struct {
	models.PackageCachingInfo
}

func (s *PackageService) GetCachedNodes(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.GetCachedNodes")

	var req GetCachedNodesReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	resp, err := s.svc.PackageSvc().GetCachedNodes(*req.UserID, *req.PackageID)
	if err != nil {
		log.Warnf("get package cached nodes failed: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package cached nodes failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(GetCachedNodesResp{resp}))
}

type GetLoadedNodesReq struct {
	UserID    *int64 `json:"userID" binding:"required"`
	PackageID *int64 `json:"packageID" binding:"required"`
}

type GetLoadedNodesResp struct {
	NodeIDs []int64 `json:"nodeIDs"`
}

func (s *PackageService) GetLoadedNodes(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.GetLoadedNodes")

	var req GetLoadedNodesReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	nodeIDs, err := s.svc.PackageSvc().GetLoadedNodes(*req.UserID, *req.PackageID)
	if err != nil {
		log.Warnf("get package loaded nodes failed: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package loaded nodes failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(GetLoadedNodesResp{
		NodeIDs: nodeIDs,
	}))
}

func mapMultiPartFileToUploadingObject(files []*multipart.FileHeader) stgiter.UploadingObjectIterator {
	return iterator.Map[*multipart.FileHeader](
		iterator.Array(files...),
		func(file *multipart.FileHeader) (*stgiter.IterUploadingObject, error) {
			stream, err := file.Open()
			if err != nil {
				return nil, err
			}

			return &stgiter.IterUploadingObject{
				Path: file.Filename,
				Size: file.Size,
				File: stream,
			}, nil
		},
	)
}
