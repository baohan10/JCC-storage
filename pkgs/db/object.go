package db

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type ObjectDB struct {
	*DB
}

func (db *DB) Object() *ObjectDB {
	return &ObjectDB{DB: db}
}

func (db *ObjectDB) GetByID(ctx SQLContext, objectID int64) (model.Object, error) {
	var ret model.Object
	err := sqlx.Get(ctx, &ret, "select * from Object where ObjectID = ?", objectID)
	return ret, err
}

func (db *ObjectDB) GetByName(ctx SQLContext, bucketID int64, name string) (model.Object, error) {
	var ret model.Object
	err := sqlx.Get(ctx, &ret, "select * from Object where BucketID = ? and Name = ?", bucketID, name)
	return ret, err
}

func (db *ObjectDB) GetBucketObjects(ctx SQLContext, userID int64, bucketID int64) ([]model.Object, error) {
	var ret []model.Object
	err := sqlx.Select(ctx, &ret, "select Object.* from UserBucket, Object where UserID = ? and UserBucket.BucketID = ? and UserBucket.BucketID = Object.BucketID", userID, bucketID)
	return ret, err
}

func (db *ObjectDB) GetByDirName(ctx SQLContext, dirName string) ([]model.Object, error) {
	var ret []model.Object
	err := sqlx.Select(ctx, &ret, "select * from Object where DirName = ? ", dirName)
	return ret, err
}

// IsAvailable 判断一个用户是否拥有指定对象
func (db *ObjectDB) IsAvailable(ctx SQLContext, userID int64, objectID int64) (bool, error) {
	var objID int64
	// 先根据ObjectID找到Object，然后判断此Object所在的Bucket是不是归此用户所有
	err := sqlx.Get(ctx, &objID,
		"select Object.ObjectID from Object, UserBucket where "+
			"Object.ObjectID = ? and "+
			"Object.BucketID = UserBucket.BucketID and "+
			"UserBucket.UserID = ?",
		objectID, userID)

	if err == sql.ErrNoRows {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("find object failed, err: %w", err)
	}

	return true, nil
}

// GetUserObject 获得Object，如果用户没有权限访问，则不会获得结果
func (db *ObjectDB) GetUserObject(ctx SQLContext, userID int64, objectID int64) (model.Object, error) {
	var ret model.Object
	err := sqlx.Get(ctx, &ret,
		"select Object.* from Object, UserBucket where "+
			"Object.ObjectID = ? and "+
			"Object.BucketID = UserBucket.BucketID and "+
			"UserBucket.UserID = ?",
		objectID, userID)
	return ret, err
}

// CreateRepObject 创建多副本对象相关的记录
func (db *ObjectDB) CreateRepObject(ctx SQLContext, bucketID int64, objectName string, fileSize int64, repCount int, nodeIDs []int64, fileHash string, dirName string) (int64, error) {
	// 根据objectname和bucketid查询，若不存在则插入，若存在则返回错误
	var objectID int64
	err := sqlx.Get(ctx, &objectID, "select ObjectID from Object where Name = ? AND BucketID = ?", objectName, bucketID)
	// 无错误代表存在记录
	if err == nil {
		return 0, fmt.Errorf("object with given Name and BucketID already exists")
	}
	// 错误不是记录不存在
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("query Object by ObjectName and BucketID failed, err: %w", err)
	}

	// 创建对象的记录
	sql := "insert into Object(Name, BucketID, State, FileSize, Redundancy, DirName) values(?,?,?,?,?,?)"
	r, err := ctx.Exec(sql, objectName, bucketID, consts.ObjectStateNormal, fileSize, models.RedundancyRep, dirName)
	if err != nil {
		return 0, fmt.Errorf("insert object failed, err: %w", err)
	}

	objectID, err = r.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get id of inserted object failed, err: %w", err)
	}

	// 创建对象副本的记录
	_, err = ctx.Exec("insert into ObjectRep(ObjectID, RepCount, FileHash) values(?,?,?)", objectID, repCount, fileHash)
	if err != nil {
		return 0, fmt.Errorf("insert object rep failed, err: %w", err)
	}

	// 创建缓存记录
	priority := 0 //优先级暂时设置为0
	for _, nodeID := range nodeIDs {
		err = db.Cache().CreatePinned(ctx, fileHash, nodeID, priority)
		if err != nil {
			return 0, fmt.Errorf("create cache failed, err: %w", err)
		}
	}

	return objectID, nil
}

func (db *ObjectDB) CreateEcObject(ctx SQLContext, bucketID int64, objectName string, fileSize int64, userID int64, nodeIDs []int64, hashs []string, ecName string, dirName string) (int64, error) {
	// 根据objectname和bucketid查询，若不存在则插入，若存在则返回错误
	var objectID int64
	err := sqlx.Get(ctx, &objectID, "select ObjectID from Object where Name = ? AND BucketID = ?", objectName, bucketID)
	// 无错误代表存在记录
	if err == nil {
		return 0, fmt.Errorf("object with given Name and BucketID already exists")
	}
	// 错误不是记录不存在
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("query Object by ObjectName and BucketID failed, err: %w", err)
	}

	// 创建对象的记录
	sql := "insert into Object(Name, BucketID, State, FileSize, Redundancy, DirName) values(?,?,?,?,?,?)"
	r, err := ctx.Exec(sql, objectName, bucketID, consts.ObjectStateNormal, fileSize, ecName, dirName)
	if err != nil {
		return 0, fmt.Errorf("insert Ec object failed, err: %w", err)
	}

	objectID, err = r.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get id of inserted object failed, err: %w", err)
	}

	// 创建编码块的记录
	for i := 0; i < len(hashs); i++ {
		_, err = ctx.Exec("insert into ObjectBlock(ObjectID, InnerID, BlockHash) values(?,?,?)", objectID, i, hashs[i])
		if err != nil {
			return 0, fmt.Errorf("insert object rep failed, err: %w", err)
		}
	}
	// 创建缓存记录
	priority := 0 //优先级暂时设置为0
	i := 0
	for _, nodeID := range nodeIDs {
		err = db.Cache().CreatePinned(ctx, hashs[i], nodeID, priority)
		i += 1
		if err != nil {
			return 0, fmt.Errorf("create cache failed, err: %w", err)
		}
	}

	return objectID, nil
}

func (db *ObjectDB) UpdateRepObject(ctx SQLContext, objectID int64, fileSize int64, nodeIDs []int64, fileHash string) error {
	obj, err := db.GetByID(ctx, objectID)
	if err != nil {
		return fmt.Errorf("get object failed, err: %w", err)
	}
	if obj.Redundancy != models.RedundancyRep {
		return fmt.Errorf("object is not a rep object")
	}

	_, err = db.UpdateFileInfo(ctx, objectID, fileSize)
	if err != nil {
		if err != nil {
			return fmt.Errorf("update rep object failed, err: %w", err)
		}
	}

	objRep, err := db.ObjectRep().GetByID(ctx, objectID)
	if err != nil {
		return fmt.Errorf("get object rep failed, err: %w", err)
	}

	// 如果新文件与旧文件的Hash不同，则需要更新关联的FileHash，设置Storage中的文件已过期，重新插入Cache记录
	if objRep.FileHash != fileHash {
		_, err := db.ObjectRep().UpdateFileHash(ctx, objectID, fileHash)
		if err != nil {
			return fmt.Errorf("update rep object file hash failed, err: %w", err)
		}

		_, err = db.StorageObject().SetAllObjectOutdated(ctx, objectID)
		if err != nil {
			return fmt.Errorf("set storage object outdated failed, err: %w", err)
		}

		for _, nodeID := range nodeIDs {
			err := db.Cache().CreatePinned(ctx, fileHash, nodeID, 0) //priority = 0
			if err != nil {
				return fmt.Errorf("create cache failed, err: %w", err)
			}
		}

	} else {
		// 如果相同，则只增加Cache中不存在的记录

		cachedNodes, err := db.Cache().GetCachingFileNodes(ctx, fileHash)
		if err != nil {
			return fmt.Errorf("find caching file nodes failed, err: %w", err)
		}

		// 筛选出不在cachedNodes中的id
		newNodeIDs := lo.Filter(nodeIDs, func(id int64, index int) bool {
			return lo.NoneBy(cachedNodes, func(node model.Node) bool {
				return node.NodeID == id
			})
		})
		for _, nodeID := range newNodeIDs {
			err := db.Cache().CreatePinned(ctx, fileHash, nodeID, 0) //priority
			if err != nil {
				return fmt.Errorf("create cache failed, err: %w", err)
			}
		}
	}

	return nil
}

// SoftDelete 设置一个对象被删除，并将相关数据删除
func (db *ObjectDB) SoftDelete(ctx SQLContext, objectID int64) error {
	obj, err := db.GetByID(ctx, objectID)
	if err != nil {
		return fmt.Errorf("get object failed, err: %w", err)
	}

	// 不是正常状态的Object，则不删除
	// TODO 未来可能有其他状态
	if obj.State != consts.ObjectStateNormal {
		return nil
	}

	err = db.ChangeState(ctx, objectID, consts.ObjectStateDeleted)
	if err != nil {
		return fmt.Errorf("change object state failed, err: %w", err)
	}

	if obj.Redundancy == models.RedundancyRep {
		err = db.ObjectRep().Delete(ctx, objectID)
		if err != nil {
			return fmt.Errorf("delete from object rep failed, err: %w", err)
		}
	} else {
		err = db.ObjectBlock().Delete(ctx, objectID)
		if err != nil {
			return fmt.Errorf("delete from object rep failed, err: %w", err)
		}
	}

	_, err = db.StorageObject().SetAllObjectDeleted(ctx, objectID)
	if err != nil {
		return fmt.Errorf("set storage object deleted failed, err: %w", err)
	}

	return nil
}

// DeleteUnused 删除一个已经是Deleted状态，且不再被使用的对象。目前可能被使用的地方只有StorageObject
func (ObjectDB) DeleteUnused(ctx SQLContext, objectID int64) error {
	_, err := ctx.Exec("delete from Object where ObjectID = ? and State = ? and "+
		"not exists(select StorageID from StorageObject where ObjectID = ?)",
		objectID,
		consts.ObjectStateDeleted,
		objectID,
	)

	return err
}

func (*ObjectDB) BatchGetAllObjectIDs(ctx SQLContext, start int, count int) ([]int64, error) {
	var ret []int64
	err := sqlx.Select(ctx, &ret, "select ObjectID from Object limit ?, ?", start, count)
	return ret, err
}

func (*ObjectDB) BatchGetAllEcObjectIDs(ctx SQLContext, start int, count int) ([]int64, error) {
	var ret []int64
	rep := "rep"
	err := sqlx.Select(ctx, &ret, "SELECT ObjectID FROM object where Redundancy != ? limit ?, ?", rep, start, count)
	return ret, err
}

func (*ObjectDB) ChangeState(ctx SQLContext, objectID int64, state string) error {
	_, err := ctx.Exec("update Object set State = ? where ObjectID = ?", state, objectID)
	return err
}

func (*ObjectDB) UpdateFileInfo(ctx SQLContext, objectID int64, fileSize int64) (bool, error) {
	ret, err := ctx.Exec("update Object set FileSize = ? where ObjectID = ?", fileSize, objectID)
	if err != nil {
		return false, err
	}

	cnt, err := ret.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("get affected rows failed, err: %w", err)
	}

	return cnt > 0, nil
}
