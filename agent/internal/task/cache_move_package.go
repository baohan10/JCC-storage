package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type CacheMovePackage struct {
	userID    cdssdk.UserID
	packageID cdssdk.PackageID
}

func NewCacheMovePackage(userID cdssdk.UserID, packageID cdssdk.PackageID) *CacheMovePackage {
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

	mutex, err := reqbuilder.NewBuilder().
		// 保护解码出来的Object数据
		IPFS().Buzy(*stgglb.Local.NodeID).
		MutexLock(ctx.distlock)
	if err != nil {
		return fmt.Errorf("acquiring distlock: %w", err)
	}
	defer mutex.Unlock()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer ipfsCli.Close()

	// TODO 可以考虑优化，比如rep类型的直接pin就可以
	objIter := ctx.downloader.DownloadPackage(t.packageID)
	defer objIter.Close()

	for {
		obj, err := objIter.MoveNext()
		if err != nil {
			if err == iterator.ErrNoMoreItem {
				break
			}
			return err
		}
		defer obj.File.Close()

		_, err = ipfsCli.CreateFile(obj.File)
		if err != nil {
			return fmt.Errorf("creating ipfs file: %w", err)
		}
	}

	_, err = coorCli.CachePackageMoved(coormq.NewCachePackageMoved(t.packageID, *stgglb.Local.NodeID))
	if err != nil {
		return fmt.Errorf("request to coordinator: %w", err)
	}

	return nil
}
