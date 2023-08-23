package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/storage-common/consts"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type StoragePackageDB struct {
	*DB
}

func (db *DB) StoragePackage() *StoragePackageDB {
	return &StoragePackageDB{DB: db}
}

func (*StoragePackageDB) Get(ctx SQLContext, storageID int64, packageID int64, userID int64) (model.StoragePackage, error) {
	var ret model.StoragePackage
	err := sqlx.Get(ctx, &ret, "select * from StoragePackage where StorageID = ? and PackageID = ? and UserID = ?", storageID, packageID, userID)
	return ret, err
}

func (*StoragePackageDB) GetAllByStorageAndPackageID(ctx SQLContext, storageID int64, packageID int64) ([]model.StoragePackage, error) {
	var ret []model.StoragePackage
	err := sqlx.Select(ctx, &ret, "select * from StoragePackage where StorageID = ? and PackageID = ?", storageID, packageID)
	return ret, err
}

func (*StoragePackageDB) GetAllByStorageID(ctx SQLContext, storageID int64) ([]model.StoragePackage, error) {
	var ret []model.StoragePackage
	err := sqlx.Select(ctx, &ret, "select * from StoragePackage where StorageID = ?", storageID)
	return ret, err
}

func (*StoragePackageDB) LoadPackage(ctx SQLContext, packageID int64, storageID int64, userID int64) error {
	_, err := ctx.Exec("insert into StoragePackage values(?,?,?,?)", packageID, storageID, userID, consts.StoragePackageStateNormal)
	return err
}

func (*StoragePackageDB) ChangeState(ctx SQLContext, storageID int64, packageID int64, userID int64, state string) error {
	_, err := ctx.Exec("update StoragePackage set State = ? where StorageID = ? and PackageID = ? and UserID = ?", state, storageID, packageID, userID)
	return err
}

// SetStateNormal 将状态设置为Normal，如果记录状态是Deleted，则不进行操作
func (*StoragePackageDB) SetStateNormal(ctx SQLContext, storageID int64, packageID int64, userID int64) error {
	_, err := ctx.Exec("update StoragePackage set State = ? where StorageID = ? and PackageID = ? and UserID = ? and State <> ?",
		consts.StoragePackageStateNormal,
		storageID,
		packageID,
		userID,
		consts.StoragePackageStateDeleted,
	)
	return err
}

func (*StoragePackageDB) SetAllPackageState(ctx SQLContext, packageID int64, state string) (int64, error) {
	ret, err := ctx.Exec(
		"update StoragePackage set State = ? where PackageID = ?",
		state,
		packageID,
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

// SetAllPackageOutdated 将Storage中指定对象设置为已过期。
// 注：只会设置Normal状态的对象
func (*StoragePackageDB) SetAllPackageOutdated(ctx SQLContext, packageID int64) (int64, error) {
	ret, err := ctx.Exec(
		"update StoragePackage set State = ? where State = ? and PackageID = ?",
		consts.StoragePackageStateOutdated,
		consts.StoragePackageStateNormal,
		packageID,
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

func (db *StoragePackageDB) SetAllPackageDeleted(ctx SQLContext, packageID int64) (int64, error) {
	return db.SetAllPackageState(ctx, packageID, consts.StoragePackageStateDeleted)
}

func (*StoragePackageDB) Delete(ctx SQLContext, storageID int64, packageID int64, userID int64) error {
	_, err := ctx.Exec("delete from StoragePackage where StorageID = ? and PackageID = ? and UserID = ?", storageID, packageID, userID)
	return err
}

// FindPackageStorages 查询存储了指定对象的Storage
func (*StoragePackageDB) FindPackageStorages(ctx SQLContext, packageID int64) ([]model.Storage, error) {
	var ret []model.Storage
	err := sqlx.Select(ctx, &ret,
		"select Storage.* from StoragePackage, Storage where PackageID = ? and"+
			" StoragePackage.StorageID = Storage.StorageID",
		packageID,
	)
	return ret, err
}
