package http

import (
	"io"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/client/internal/task"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

type ObjectService struct {
	*Server
}

func (s *Server) ObjectSvc() *ObjectService {
	return &ObjectService{
		Server: s,
	}
}

type ObjectDownloadReq struct {
	UserID   *int64 `form:"userID" binding:"required"`
	ObjectID *int64 `form:"objectID" binding:"required"`
}

func (s *ObjectService) Download(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Download")

	var req ObjectDownloadReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	file, err := s.svc.ObjectSvc().DownloadObject(*req.UserID, *req.ObjectID)
	if err != nil {
		log.Warnf("downloading object: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "download object failed"))
		return
	}

	ctx.Writer.WriteHeader(http.StatusOK)
	// TODO 需要设置FileName
	ctx.Header("Content-Disposition", "attachment; filename=filename")
	ctx.Header("Content-Type", "application/octet-stream")

	buf := make([]byte, 4096)
	ctx.Stream(func(w io.Writer) bool {
		rd, err := file.Read(buf)
		if err == io.EOF {
			return false
		}

		if err != nil {
			log.Warnf("reading file data: %s", err.Error())
			return false
		}

		err = myio.WriteAll(w, buf[:rd])
		if err != nil {
			log.Warnf("writing data to response: %s", err.Error())
			return false
		}

		return true
	})
}

type ObjectUploadReq struct {
	Info ObjectUploadInfo      `form:"info" binding:"required"`
	File *multipart.FileHeader `form:"file"`
}

type ObjectUploadInfo struct {
	UserID     *int64 `json:"userID" binding:"required"`
	BucketID   *int64 `json:"bucketID" binding:"required"`
	FileSize   *int64 `json:"fileSize" binding:"required"`
	ObjectName string `json:"objectName" binding:"required"`
	Redundancy struct {
		Type   string `json:"type" binding:"required"`
		Config any    `json:"config" binding:"required"`
	} `json:"redundancy" binding:"required"`
}

type ObjectUploadResp struct {
	ObjectID int64 `json:"objectID,string"`
}

func (s *ObjectService) Upload(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Upload")

	var req ObjectUploadReq
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

	}

	ctx.JSON(http.StatusForbidden, Failed(errorcode.OperationFailed, "not supported yet"))
}

func (s *ObjectService) uploadRep(ctx *gin.Context, req *ObjectUploadReq) {
	log := logger.WithField("HTTP", "Object.Upload")

	var repInfo models.RepRedundancyConfig
	if err := serder.AnyToAny(req.Info.Redundancy.Config, &repInfo); err != nil {
		log.Warnf("parsing rep redundancy config: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "invalid rep redundancy config"))
		return
	}

	file, err := req.File.Open()
	if err != nil {
		log.Warnf("opening file: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "open file failed"))
		return
	}

	taskID, err := s.svc.ObjectSvc().StartUploadingRepObjects(*req.Info.UserID, *req.Info.BucketID, []task.UploadObject{{
		ObjectName: req.Info.ObjectName,
		File:       file,
		FileSize:   *req.Info.FileSize,
	}}, repInfo.RepCount)

	if err != nil {
		log.Warnf("start uploading rep object task: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "start uploading task failed"))
		return
	}

	for {
		complete, uploadObjectResult, err := s.svc.ObjectSvc().WaitUploadingRepObjects(taskID, time.Second*5)
		if complete {
			if err != nil {
				log.Warnf("uploading rep object: %s", err.Error())
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "uploading rep object failed"))
				return
			}

			uploadRet := uploadObjectResult.Results[0]
			if uploadRet.Error != nil {
				log.Warnf("uploading rep object: %s", uploadRet.Error)
				ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, uploadRet.Error.Error()))
				return
			}

			ctx.JSON(http.StatusOK, OK(ObjectUploadResp{
				ObjectID: uploadRet.ObjectID,
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

type ObjectDeleteReq struct {
	UserID   *int64 `json:"userID" binding:"required"`
	ObjectID *int64 `json:"objectID" binding:"required"`
}

func (s *ObjectService) Delete(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.Delete")

	var req ObjectDeleteReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		log.Warnf("binding body: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	err := s.svc.ObjectSvc().DeleteObject(*req.UserID, *req.ObjectID)
	if err != nil {
		log.Warnf("deleting object: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "delete object failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(nil))
}
