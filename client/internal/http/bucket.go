package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type BucketService struct {
	*Server
}

func (s *Server) Bucket() *BucketService {
	return &BucketService{
		Server: s,
	}
}

func (s *BucketService) GetByName(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.GetByName")

	var req cdssdk.BucketGetByName
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	bucket, err := s.svc.BucketSvc().GetBucketByName(req.UserID, req.Name)
	if err != nil {
		log.Warnf("getting bucket by name: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get bucket by name failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdssdk.BucketGetByNameResp{
		Bucket: bucket,
	}))
}

func (s *BucketService) Create(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.Create")

	var req cdssdk.BucketCreate
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	bucket, err := s.svc.BucketSvc().CreateBucket(req.UserID, req.Name)
	if err != nil {
		log.Warnf("creating bucket: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "create bucket failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdssdk.BucketCreateResp{
		Bucket: bucket,
	}))
}

func (s *BucketService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.Delete")

	var req cdssdk.BucketDelete
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	if err := s.svc.BucketSvc().DeleteBucket(req.UserID, req.BucketID); err != nil {
		log.Warnf("deleting bucket: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "delete bucket failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
}

func (s *BucketService) ListUserBuckets(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.ListUserBuckets")

	var req cdssdk.BucketListUserBucketsReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	buckets, err := s.svc.BucketSvc().GetUserBuckets(req.UserID)
	if err != nil {
		log.Warnf("getting user buckets: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get user buckets failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(cdssdk.BucketListUserBucketsResp{
		Buckets: buckets,
	}))
}
