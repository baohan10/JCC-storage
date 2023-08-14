package http

import (
	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage-client/internal/services"
)

type Server struct {
	engine     *gin.Engine
	listenAddr string
	svc        *services.Service
}

func NewServer(listenAddr string, svc *services.Service) (*Server, error) {
	engine := gin.New()

	return &Server{
		engine:     engine,
		listenAddr: listenAddr,
		svc:        svc,
	}, nil
}

func (s *Server) Serve() error {
	s.initRouters()

	logger.Infof("start serving http at: %s", s.listenAddr)
	err := s.engine.Run(s.listenAddr)

	if err != nil {
		logger.Infof("http stopped with error: %s", err.Error())
		return err
	}

	logger.Infof("http stopped")
	return nil
}

func (s *Server) initRouters() {
	s.engine.GET("/object/download", s.ObjectSvc().Download)
	s.engine.POST("/object/upload", s.ObjectSvc().Upload)
	s.engine.POST("/object/delete", s.ObjectSvc().Delete)

	s.engine.POST("/storage/moveObject", s.StorageSvc().MoveObject)
}
