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

	s.engine.POST("/package/upload", s.PackageSvc().Upload)
	s.engine.POST("/package/delete", s.PackageSvc().Delete)
	s.engine.GET("/package/getCacheNodeIDs", s.PackageSvc().GetCacheNodeIDs)
	s.engine.GET("/package/getStorageNodeIDs", s.PackageSvc().GetStorageNodeIDs)

	s.engine.POST("/storage/loadPackage", s.StorageSvc().LoadPackage)
	s.engine.POST("/storage/createPackage", s.StorageSvc().CreatePackage)

	s.engine.POST("/cache/movePackage", s.CacheSvc().MovePackage)
}
