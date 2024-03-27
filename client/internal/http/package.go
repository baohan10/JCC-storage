package http

import (
	"mime/multipart"
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	stgiter "gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

type PackageService struct {
	*Server
}

func (s *Server) Package() *PackageService {
	return &PackageService{
		Server: s,
	}
}

type PackageGetReq struct {
	UserID    *cdssdk.UserID    `form:"userID" binding:"required"`
	PackageID *cdssdk.PackageID `form:"packageID" binding:"required"`
}
type PackageGetResp struct {
	model.Package
}

func (s *PackageService) Get(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.Get")

	var req PackageGetReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	pkg, err := s.svc.PackageSvc().Get(*req.UserID, *req.PackageID)
	if err != nil {
		log.Warnf("getting package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get package failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(PackageGetResp{Package: *pkg}))
}

func (s *PackageService) Create(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Package.Create")
	var req cdssdk.PackageCreateReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	pkgID, err := s.svc.PackageSvc().Create(req.UserID, req.BucketID, req.Name)
	if err != nil {
		log.Warnf("creating package: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "create package failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdssdk.PackageCreateResp{
		PackageID: pkgID,
	}))
}

type PackageDeleteReq struct {
	UserID    *cdssdk.UserID    `json:"userID" binding:"required"`
	PackageID *cdssdk.PackageID `json:"packageID" binding:"required"`
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
	UserID    *cdssdk.UserID    `json:"userID" binding:"required"`
	PackageID *cdssdk.PackageID `json:"packageID" binding:"required"`
}
type GetCachedNodesResp struct {
	cdssdk.PackageCachingInfo
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
	UserID    *cdssdk.UserID    `json:"userID" binding:"required"`
	PackageID *cdssdk.PackageID `json:"packageID" binding:"required"`
}

type GetLoadedNodesResp struct {
	NodeIDs []cdssdk.NodeID `json:"nodeIDs"`
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
