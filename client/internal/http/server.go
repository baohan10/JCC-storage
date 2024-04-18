package http

import (
	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/client/internal/services"
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
	s.engine.GET(cdssdk.ObjectDownloadPath, s.Object().Download)
	s.engine.POST(cdssdk.ObjectUploadPath, s.Object().Upload)
	s.engine.GET(cdssdk.ObjectGetPackageObjectsPath, s.Object().GetPackageObjects)
	s.engine.POST(cdssdk.ObjectUpdateInfoPath, s.Object().UpdateInfo)
	s.engine.POST(cdssdk.ObjectMovePath, s.Object().Move)
	s.engine.POST(cdssdk.ObjectDeletePath, s.Object().Delete)

	s.engine.GET(cdssdk.PackageGetPath, s.Package().Get)
	s.engine.GET(cdssdk.PackageGetByNamePath, s.Package().GetByName)
	s.engine.POST(cdssdk.PackageCreatePath, s.Package().Create)
	s.engine.POST(cdssdk.PackageDeletePath, s.Package().Delete)
	s.engine.GET(cdssdk.PackageListBucketPackagesPath, s.Package().ListBucketPackages)
	s.engine.GET(cdssdk.PackageGetCachedNodesPath, s.Package().GetCachedNodes)
	s.engine.GET(cdssdk.PackageGetLoadedNodesPath, s.Package().GetLoadedNodes)

	s.engine.POST(cdssdk.StorageLoadPackagePath, s.Storage().LoadPackage)
	s.engine.POST(cdssdk.StorageCreatePackagePath, s.Storage().CreatePackage)
	s.engine.GET(cdssdk.StorageGetInfoPath, s.Storage().GetInfo)

	s.engine.POST(cdssdk.CacheMovePackagePath, s.Cache().MovePackage)

	s.engine.GET(cdssdk.BucketGetByNamePath, s.Bucket().GetByName)
	s.engine.POST(cdssdk.BucketCreatePath, s.Bucket().Create)
	s.engine.POST(cdssdk.BucketDeletePath, s.Bucket().Delete)
	s.engine.GET(cdssdk.BucketListUserBucketsPath, s.Bucket().ListUserBuckets)
}
