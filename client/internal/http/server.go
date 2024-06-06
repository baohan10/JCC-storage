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
	rt := s.engine.Use()

	initTemp(rt, s)

	rt.GET(cdssdk.ObjectDownloadPath, s.Object().Download)
	rt.POST(cdssdk.ObjectUploadPath, s.Object().Upload)
	rt.GET(cdssdk.ObjectGetPackageObjectsPath, s.Object().GetPackageObjects)
	rt.POST(cdssdk.ObjectUpdateInfoPath, s.Object().UpdateInfo)
	rt.POST(cdssdk.ObjectMovePath, s.Object().Move)
	rt.POST(cdssdk.ObjectDeletePath, s.Object().Delete)

	rt.GET(cdssdk.PackageGetPath, s.Package().Get)
	rt.GET(cdssdk.PackageGetByNamePath, s.Package().GetByName)
	rt.POST(cdssdk.PackageCreatePath, s.Package().Create)
	rt.POST(cdssdk.PackageDeletePath, s.Package().Delete)
	rt.GET(cdssdk.PackageListBucketPackagesPath, s.Package().ListBucketPackages)
	rt.GET(cdssdk.PackageGetCachedNodesPath, s.Package().GetCachedNodes)
	rt.GET(cdssdk.PackageGetLoadedNodesPath, s.Package().GetLoadedNodes)

	rt.POST(cdssdk.StorageLoadPackagePath, s.Storage().LoadPackage)
	rt.POST(cdssdk.StorageCreatePackagePath, s.Storage().CreatePackage)
	rt.GET(cdssdk.StorageGetPath, s.Storage().Get)

	rt.POST(cdssdk.CacheMovePackagePath, s.Cache().MovePackage)

	rt.GET(cdssdk.BucketGetByNamePath, s.Bucket().GetByName)
	rt.POST(cdssdk.BucketCreatePath, s.Bucket().Create)
	rt.POST(cdssdk.BucketDeletePath, s.Bucket().Delete)
	rt.GET(cdssdk.BucketListUserBucketsPath, s.Bucket().ListUserBuckets)
}
