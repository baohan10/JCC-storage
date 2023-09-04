package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type CacheMovePackage struct {
	userID    int64
	packageID int64
}

func NewCacheMovePackage(userID int64, packageID int64) *CacheMovePackage {
	return &CacheMovePackage{
		userID:    userID,
		packageID: packageID,
	}
}

func (t *CacheMovePackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *CacheMovePackage) do(ctx TaskContext) error {
	log := logger.WithType[CacheMovePackage]("Task")
	log.Debugf("begin with %v", logger.FormatStruct(t))
	defer log.Debugf("end")

	// TOOD EC的锁
	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 读取Package信息和包含的Object信息
		Package().ReadOne(t.packageID).Object().ReadAny().
		// 读取Rep对象的配置
		ObjectRep().ReadAny().
		// 创建Cache记录
		Cache().CreateAny().
		IPFS().
		// pin文件
		CreateAnyRep(*globals.Local.NodeID).
		MutexLock(ctx.distlock)
	if err != nil {
		return fmt.Errorf("acquiring distlock: %w", err)
	}
	defer mutex.Unlock()

	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer coorCli.Close()

	pkgResp, err := coorCli.GetPackage(coormq.NewGetPackage(t.userID, t.packageID))
	if err != nil {
		return fmt.Errorf("getting package: %w", err)
	}

	if pkgResp.Redundancy.IsRepInfo() {
		return t.moveRep(ctx, coorCli, pkgResp.Package)
	} else {
		// TODO EC的CacheMove逻辑
	}

	return nil
}
func (t *CacheMovePackage) moveRep(ctx TaskContext, coorCli *coormq.PoolClient, pkg model.Package) error {
	getRepResp, err := coorCli.GetPackageObjectRepData(coormq.NewGetPackageObjectRepData(pkg.PackageID))
	if err != nil {
		return fmt.Errorf("getting package object rep data: %w", err)
	}

	ipfsCli, err := globals.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer ipfsCli.Close()

	var fileHashes []string
	for _, rep := range getRepResp.Data {
		if err := ipfsCli.Pin(rep.FileHash); err != nil {
			return fmt.Errorf("pinning file %s: %w", rep.FileHash, err)
		}

		fileHashes = append(fileHashes, rep.FileHash)
	}

	_, err = coorCli.CachePackageMoved(coormq.NewCachePackageMoved(pkg.PackageID, *globals.Local.NodeID, fileHashes))
	if err != nil {
		return fmt.Errorf("reporting cache package moved: %w", err)
	}

	return nil
}
