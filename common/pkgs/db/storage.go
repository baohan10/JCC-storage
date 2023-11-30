package db

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type StorageDB struct {
	*DB
}

func (db *DB) Storage() *StorageDB {
	return &StorageDB{DB: db}
}

func (db *StorageDB) GetByID(ctx SQLContext, stgID cdssdk.StorageID) (model.Storage, error) {
	var stg model.Storage
	err := sqlx.Get(ctx, &stg, "select * from Storage where StorageID = ?", stgID)
	return stg, err
}

func (db *StorageDB) BatchGetAllStorageIDs(ctx SQLContext, start int, count int) ([]cdssdk.StorageID, error) {
	var ret []cdssdk.StorageID
	err := sqlx.Select(ctx, &ret, "select StorageID from Storage limit ?, ?", start, count)
	return ret, err
}

func (db *StorageDB) IsAvailable(ctx SQLContext, userID cdssdk.UserID, storageID cdssdk.StorageID) (bool, error) {
	var stgID int64
	err := sqlx.Get(ctx, &stgID,
		"select Storage.StorageID from Storage, UserStorage where"+
			" Storage.StorageID = ? and"+
			" Storage.StorageID = UserStorage.StorageID and"+
			" UserStorage.UserID = ?",
		storageID, userID)

	if err == sql.ErrNoRows {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("find storage failed, err: %w", err)
	}

	return true, nil
}

func (db *StorageDB) GetUserStorage(ctx SQLContext, userID cdssdk.UserID, storageID cdssdk.StorageID) (model.Storage, error) {
	var stg model.Storage
	err := sqlx.Get(ctx, &stg,
		"select Storage.* from UserStorage, Storage where UserID = ? and UserStorage.StorageID = ? and UserStorage.StorageID = Storage.StorageID",
		userID,
		storageID)

	return stg, err
}

func (db *StorageDB) ChangeState(ctx SQLContext, storageID cdssdk.StorageID, state string) error {
	_, err := ctx.Exec("update Storage set State = ? where StorageID = ?", state, storageID)
	return err
}
