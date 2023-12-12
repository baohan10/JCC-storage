package db

import (
	"time"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type StoragePackageLogDB struct {
	*DB
}

func (db *DB) StoragePackageLog() *StoragePackageLogDB {
	return &StoragePackageLogDB{DB: db}
}

func (*StoragePackageLogDB) Get(ctx SQLContext, storageID cdssdk.StorageID, packageID cdssdk.PackageID, userID cdssdk.UserID) (model.StoragePackageLog, error) {
	var ret model.StoragePackageLog
	err := sqlx.Get(ctx, &ret, "select * from StoragePackageLog where StorageID = ? and PackageID = ? and UserID = ?", storageID, packageID, userID)
	return ret, err
}

func (*StoragePackageLogDB) GetByPackageID(ctx SQLContext, packageID cdssdk.PackageID) ([]model.StoragePackageLog, error) {
	var ret []model.StoragePackageLog
	err := sqlx.Select(ctx, &ret, "select * from StoragePackageLog where PackageID = ?", packageID)
	return ret, err
}

func (*StoragePackageLogDB) Create(ctx SQLContext, storageID cdssdk.StorageID, packageID cdssdk.PackageID, userID cdssdk.UserID, createTime time.Time) error {
	_, err := ctx.Exec("insert into StoragePackageLog values(?,?,?,?)", storageID, packageID, userID, createTime)
	return err
}

func (*StoragePackageLogDB) Delete(ctx SQLContext, storageID cdssdk.StorageID, packageID cdssdk.PackageID, userID cdssdk.UserID) error {
	_, err := ctx.Exec("delete from StoragePackageLog where StorageID = ? and PackageID = ? and UserID = ?", storageID, packageID, userID)
	return err
}
