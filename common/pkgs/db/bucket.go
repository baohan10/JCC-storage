package db

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type BucketDB struct {
	*DB
}

func (db *DB) Bucket() *BucketDB {
	return &BucketDB{DB: db}
}

func (db *BucketDB) GetByID(ctx SQLContext, bucketID cdssdk.BucketID) (cdssdk.Bucket, error) {
	var ret cdssdk.Bucket
	err := sqlx.Get(ctx, &ret, "select * from Bucket where BucketID = ?", bucketID)
	return ret, err
}

// GetIDByName 根据BucketName查询BucketID
func (db *BucketDB) GetIDByName(bucketName string) (int64, error) {
	//桶结构体
	var result struct {
		BucketID   int64  `db:"BucketID"`
		BucketName string `db:"BucketName"`
	}

	sql := "select BucketID, BucketName from Bucket where BucketName=? "
	if err := db.d.Get(&result, sql, bucketName); err != nil {
		return 0, err
	}

	return result.BucketID, nil
}

// IsAvailable 判断用户是否有指定Bucekt的权限
func (db *BucketDB) IsAvailable(ctx SQLContext, bucketID cdssdk.BucketID, userID cdssdk.UserID) (bool, error) {
	_, err := db.GetUserBucket(ctx, userID, bucketID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("find bucket failed, err: %w", err)
	}

	return true, nil
}

func (*BucketDB) GetUserBucket(ctx SQLContext, userID cdssdk.UserID, bucketID cdssdk.BucketID) (model.Bucket, error) {
	var ret model.Bucket
	err := sqlx.Get(ctx, &ret,
		"select Bucket.* from UserBucket, Bucket where UserID = ? and"+
			" UserBucket.BucketID = Bucket.BucketID and"+
			" Bucket.BucketID = ?", userID, bucketID)
	return ret, err
}

func (*BucketDB) GetUserBucketByName(ctx SQLContext, userID cdssdk.UserID, bucketName string) (model.Bucket, error) {
	var ret model.Bucket
	err := sqlx.Get(ctx, &ret,
		"select Bucket.* from UserBucket, Bucket where UserID = ? and"+
			" UserBucket.BucketID = Bucket.BucketID and"+
			" Bucket.Name = ?", userID, bucketName)
	return ret, err
}

func (*BucketDB) GetUserBuckets(ctx SQLContext, userID cdssdk.UserID) ([]model.Bucket, error) {
	var ret []model.Bucket
	err := sqlx.Select(ctx, &ret, "select Bucket.* from UserBucket, Bucket where UserID = ? and UserBucket.BucketID = Bucket.BucketID", userID)
	return ret, err
}

func (db *BucketDB) Create(ctx SQLContext, userID cdssdk.UserID, bucketName string) (cdssdk.BucketID, error) {
	var bucketID int64
	err := sqlx.Get(ctx, &bucketID, "select Bucket.BucketID from UserBucket, Bucket where UserBucket.UserID = ? and UserBucket.BucketID = Bucket.BucketID and Bucket.Name = ?", userID, bucketName)
	if err == nil {
		return 0, fmt.Errorf("bucket name exsits")
	}

	if err != sql.ErrNoRows {
		return 0, err
	}

	ret, err := ctx.Exec("insert into Bucket(Name,CreatorID) values(?,?)", bucketName, userID)
	if err != nil {
		return 0, fmt.Errorf("insert bucket failed, err: %w", err)
	}

	bucketID, err = ret.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get inserted bucket id failed, err: %w", err)
	}

	_, err = ctx.Exec("insert into UserBucket(UserID,BucketID) values(?,?)", userID, bucketID)
	if err != nil {
		return 0, fmt.Errorf("insert into user bucket failed, err: %w", err)
	}

	return cdssdk.BucketID(bucketID), err
}

func (db *BucketDB) Delete(ctx SQLContext, bucketID cdssdk.BucketID) error {
	_, err := ctx.Exec("delete from UserBucket where BucketID = ?", bucketID)
	if err != nil {
		return fmt.Errorf("delete user bucket failed, err: %w", err)
	}

	_, err = ctx.Exec("delete from Bucket where BucketID = ?", bucketID)
	if err != nil {
		return fmt.Errorf("delete bucket failed, err: %w", err)
	}

	// 删除Bucket内的Package
	var pkgIDs []cdssdk.PackageID
	err = sqlx.Select(ctx, &pkgIDs, "select PackageID from Package where BucketID = ?", bucketID)
	if err != nil {
		return fmt.Errorf("query package failed, err: %w", err)
	}

	for _, pkgID := range pkgIDs {
		err = db.Package().SoftDelete(ctx, pkgID)
		if err != nil {
			return fmt.Errorf("set package seleted failed, err: %w", err)
		}

		// 失败也没关系，会有定时任务再次尝试
		db.Package().DeleteUnused(ctx, pkgID)
	}
	return nil
}
