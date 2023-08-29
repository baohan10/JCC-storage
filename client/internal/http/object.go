package http

import (
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	myio "gitlink.org.cn/cloudream/common/utils/io"
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

	file, err := s.svc.ObjectSvc().Download(*req.UserID, *req.ObjectID)
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
