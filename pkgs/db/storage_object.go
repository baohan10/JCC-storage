package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type StorageObjectDB struct {
	*DB
}

func (db *DB) StorageObject() *StorageObjectDB {
	return &StorageObjectDB{DB: db}
}

func (*StorageObjectDB) Get(ctx SQLContext, storageID int64, objectID int64, userID int64) (model.StorageObject, error) {
	var ret model.StorageObject
	err := sqlx.Get(ctx, &ret, "select * from StorageObject where StorageID = ? and ObjectID = ? and UserID = ?", storageID, objectID, userID)
	return ret, err
}

func (*StorageObjectDB) GetAllByStorageAndObjectID(ctx SQLContext, storageID int64, objectID int64) ([]model.StorageObject, error) {
	var ret []model.StorageObject
	err := sqlx.Select(ctx, &ret, "select * from StorageObject where StorageID = ? and ObjectID = ?", storageID, objectID)
	return ret, err
}

func (*StorageObjectDB) GetAllByStorageID(ctx SQLContext, storageID int64) ([]model.StorageObject, error) {
	var ret []model.StorageObject
	err := sqlx.Select(ctx, &ret, "select * from StorageObject where StorageID = ?", storageID)
	return ret, err
}

func (*StorageObjectDB) MoveObjectTo(ctx SQLContext, objectID int64, storageID int64, userID int64) error {
	_, err := ctx.Exec("insert into StorageObject values(?,?,?,?)", objectID, storageID, userID, consts.StorageObjectStateNormal)
	return err
}

func (*StorageObjectDB) ChangeState(ctx SQLContext, storageID int64, objectID int64, userID int64, state string) error {
	_, err := ctx.Exec("update StorageObject set State = ? where StorageID = ? and ObjectID = ? and UserID = ?", state, storageID, objectID, userID)
	return err
}

// SetStateNormal 将状态设置为Normal，如果记录状态是Deleted，则不进行操作
func (*StorageObjectDB) SetStateNormal(ctx SQLContext, storageID int64, objectID int64, userID int64) error {
	_, err := ctx.Exec("update StorageObject set State = ? where StorageID = ? and ObjectID = ? and UserID = ? and State <> ?",
		consts.StorageObjectStateNormal,
		storageID,
		objectID,
		userID,
		consts.StorageObjectStateDeleted,
	)
	return err
}

func (*StorageObjectDB) SetAllObjectState(ctx SQLContext, objectID int64, state string) (int64, error) {
	ret, err := ctx.Exec(
		"update StorageObject set State = ? where ObjectID = ?",
		state,
		objectID,
	)
	if err != nil {
		return 0, err
	}

	cnt, err := ret.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get affected rows failed, err: %w", err)
	}

	return cnt, nil
}

// SetAllObjectOutdated 将Storage中指定对象设置为已过期。
// 注：只会设置Normal状态的对象
func (*StorageObjectDB) SetAllObjectOutdated(ctx SQLContext, objectID int64) (int64, error) {
	ret, err := ctx.Exec(
		"update StorageObject set State = ? where State = ? and ObjectID = ?",
		consts.StorageObjectStateOutdated,
		consts.StorageObjectStateNormal,
		objectID,
	)
	if err != nil {
		return 0, err
	}

	cnt, err := ret.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get affected rows failed, err: %w", err)
	}

	return cnt, nil
}

func (db *StorageObjectDB) SetAllObjectDeleted(ctx SQLContext, objectID int64) (int64, error) {
	return db.SetAllObjectState(ctx, objectID, consts.StorageObjectStateDeleted)
}

func (*StorageObjectDB) Delete(ctx SQLContext, storageID int64, objectID int64, userID int64) error {
	_, err := ctx.Exec("delete from StorageObject where StorageID = ? and ObjectID = ? and UserID = ?", storageID, objectID, userID)
	return err
}

// FindObjectStorages 查询存储了指定对象的Storage
func (*StorageObjectDB) FindObjectStorages(ctx SQLContext, objectID int64) ([]model.Storage, error) {
	var ret []model.Storage
	err := sqlx.Select(ctx, &ret,
		"select Storage.* from StorageObject, Storage where ObjectID = ? and "+
			"StorageObject.StorageID = Storage.StorageID",
		objectID,
	)
	return ret, err
}
