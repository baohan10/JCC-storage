package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"
	coormsg "gitlink.org.cn/cloudream/storage-common/pkgs/mq/message/coordinator"
)

type MoveDirToStorage struct {
	userID                 int64
	dirName                string
	storageID              int64
	ResultObjectToStorages []ResultObjectToStorage
}

type ResultObjectToStorage struct {
	ObjectName string
	Error      error
}

func NewMoveDirToStorage(userID int64, dirName string, storageID int64) *MoveDirToStorage {
	return &MoveDirToStorage{
		userID:    userID,
		dirName:   dirName,
		storageID: storageID,
	}
}

func (t *MoveDirToStorage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *MoveDirToStorage) do(ctx TaskContext) error {
	//根据dirName查询相关的所有文件
	objsResp, err := ctx.Coordinator.GetObjectsByDirName(coormsg.NewGetObjectsByDirName(t.userID, t.dirName))
	if err != nil {
		return fmt.Errorf("get objectID by dirName failed: %w", err)
	}
	if len(objsResp.Objects) == 0 {
		return fmt.Errorf("dirName %v is not exist", t.dirName)
	}

	reqBlder := reqbuilder.NewBuilder()
	for _, object := range objsResp.Objects {
		reqBlder.Metadata().
			// 用于判断用户是否有Storage权限
			UserStorage().ReadOne(object.ObjectID, t.storageID).
			// 用于读取对象信息
			Object().ReadOne(object.ObjectID).
			// 用于查询Rep配置
			ObjectRep().ReadOne(object.ObjectID).
			// 用于创建Move记录
			StorageObject().CreateOne(t.storageID, t.userID, object.ObjectID).
			// 用于创建对象文件
			Storage().CreateOneObject(t.storageID, t.userID, object.ObjectID)
	}
	mutex, err := reqBlder.
		Metadata().
		// 用于判断用户是否有对象权限
		UserBucket().ReadAny().
		// 用于查询Block配置
		ObjectBlock().ReadAny().
		MutexLock(ctx.DistLock)

	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	for i := 0; i < len(objsResp.Objects); i++ {
		err := moveSingleObjectToStorage(ctx, t.userID, objsResp.Objects[i].ObjectID, t.storageID)
		t.ResultObjectToStorages = append(t.ResultObjectToStorages, ResultObjectToStorage{
			ObjectName: objsResp.Objects[i].Name,
			Error:      err,
		})
	}
	return nil
}
