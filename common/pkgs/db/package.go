package db

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type PackageDB struct {
	*DB
}

func (db *DB) Package() *PackageDB {
	return &PackageDB{DB: db}
}

func (db *PackageDB) GetByID(ctx SQLContext, packageID cdssdk.PackageID) (model.Package, error) {
	var ret model.Package
	err := sqlx.Get(ctx, &ret, "select * from Package where PackageID = ?", packageID)
	return ret, err
}

func (db *PackageDB) GetByName(ctx SQLContext, bucketID cdssdk.BucketID, name string) (model.Package, error) {
	var ret model.Package
	err := sqlx.Get(ctx, &ret, "select * from Package where BucketID = ? and Name = ?", bucketID, name)
	return ret, err
}

func (*PackageDB) BatchGetAllPackageIDs(ctx SQLContext, start int, count int) ([]cdssdk.PackageID, error) {
	var ret []cdssdk.PackageID
	err := sqlx.Select(ctx, &ret, "select PackageID from Package limit ?, ?", start, count)
	return ret, err
}

func (db *PackageDB) GetBucketPackages(ctx SQLContext, userID cdssdk.UserID, bucketID cdssdk.BucketID) ([]model.Package, error) {
	var ret []model.Package
	err := sqlx.Select(ctx, &ret, "select Package.* from UserBucket, Package where UserID = ? and UserBucket.BucketID = ? and UserBucket.BucketID = Package.BucketID", userID, bucketID)
	return ret, err
}

// IsAvailable 判断一个用户是否拥有指定对象
func (db *PackageDB) IsAvailable(ctx SQLContext, userID cdssdk.UserID, packageID cdssdk.PackageID) (bool, error) {
	var pkgID cdssdk.PackageID
	// 先根据PackageID找到Package，然后判断此Package所在的Bucket是不是归此用户所有
	err := sqlx.Get(ctx, &pkgID,
		"select Package.PackageID from Package, UserBucket where "+
			"Package.PackageID = ? and "+
			"Package.BucketID = UserBucket.BucketID and "+
			"UserBucket.UserID = ?",
		packageID, userID)

	if err == sql.ErrNoRows {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("find package failed, err: %w", err)
	}

	return true, nil
}

// GetUserPackage 获得Package，如果用户没有权限访问，则不会获得结果
func (db *PackageDB) GetUserPackage(ctx SQLContext, userID cdssdk.UserID, packageID cdssdk.PackageID) (model.Package, error) {
	var ret model.Package
	err := sqlx.Get(ctx, &ret,
		"select Package.* from Package, UserBucket where"+
			" Package.PackageID = ? and"+
			" Package.BucketID = UserBucket.BucketID and"+
			" UserBucket.UserID = ?",
		packageID, userID)
	return ret, err
}

func (db *PackageDB) Create(ctx SQLContext, bucketID cdssdk.BucketID, name string) (cdssdk.PackageID, error) {
	// 根据packagename和bucketid查询，若不存在则插入，若存在则返回错误
	var packageID int64
	err := sqlx.Get(ctx, &packageID, "select PackageID from Package where Name = ? AND BucketID = ? for update", name, bucketID)
	// 无错误代表存在记录
	if err == nil {
		return 0, fmt.Errorf("package with given Name and BucketID already exists")
	}
	// 错误不是记录不存在
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("query Package by PackageName and BucketID failed, err: %w", err)
	}

	sql := "insert into Package(Name, BucketID, State) values(?,?,?)"
	r, err := ctx.Exec(sql, name, bucketID, cdssdk.PackageStateNormal)
	if err != nil {
		return 0, fmt.Errorf("insert package failed, err: %w", err)
	}

	packageID, err = r.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get id of inserted package failed, err: %w", err)
	}

	return cdssdk.PackageID(packageID), nil
}

// SoftDelete 设置一个对象被删除，并将相关数据删除
func (db *PackageDB) SoftDelete(ctx SQLContext, packageID cdssdk.PackageID) error {
	obj, err := db.GetByID(ctx, packageID)
	if err != nil {
		return fmt.Errorf("get package failed, err: %w", err)
	}

	// 不是正常状态的Package，则不删除
	// TODO 未来可能有其他状态
	if obj.State != cdssdk.PackageStateNormal {
		return nil
	}

	err = db.ChangeState(ctx, packageID, cdssdk.PackageStateDeleted)
	if err != nil {
		return fmt.Errorf("change package state failed, err: %w", err)
	}

	err = db.ObjectBlock().DeleteInPackage(ctx, packageID)
	if err != nil {
		return fmt.Errorf("delete from object rep failed, err: %w", err)
	}

	if err := db.PinnedObject().DeleteInPackage(ctx, packageID); err != nil {
		return fmt.Errorf("deleting pinned objects in package: %w", err)
	}

	if err := db.Object().DeleteInPackage(ctx, packageID); err != nil {
		return fmt.Errorf("deleting objects in package: %w", err)
	}

	_, err = db.StoragePackage().SetAllPackageDeleted(ctx, packageID)
	if err != nil {
		return fmt.Errorf("set storage package deleted failed, err: %w", err)
	}

	return nil
}

// DeleteUnused 删除一个已经是Deleted状态，且不再被使用的对象。目前可能被使用的地方只有StoragePackage
func (PackageDB) DeleteUnused(ctx SQLContext, packageID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete from Package where PackageID = ? and State = ? and "+
		"not exists(select StorageID from StoragePackage where PackageID = ?)",
		packageID,
		cdssdk.PackageStateDeleted,
		packageID,
	)

	return err
}

func (*PackageDB) ChangeState(ctx SQLContext, packageID cdssdk.PackageID, state string) error {
	_, err := ctx.Exec("update Package set State = ? where PackageID = ?", state, packageID)
	return err
}
