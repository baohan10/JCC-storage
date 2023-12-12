package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type ObjectDB struct {
	*DB
}

func (db *DB) Object() *ObjectDB {
	return &ObjectDB{DB: db}
}

func (db *ObjectDB) GetByID(ctx SQLContext, objectID cdssdk.ObjectID) (model.Object, error) {
	var ret model.TempObject
	err := sqlx.Get(ctx, &ret, "select * from Object where ObjectID = ?", objectID)
	return ret.ToObject(), err
}

func (db *ObjectDB) Create(ctx SQLContext, packageID cdssdk.PackageID, path string, size int64, fileHash string, redundancy cdssdk.Redundancy) (int64, error) {
	sql := "insert into Object(PackageID, Path, Size, FileHash, Redundancy) values(?,?,?,?,?)"

	ret, err := ctx.Exec(sql, packageID, path, size, redundancy)
	if err != nil {
		return 0, fmt.Errorf("insert object failed, err: %w", err)
	}

	objectID, err := ret.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get id of inserted object failed, err: %w", err)
	}

	return objectID, nil
}

// 创建或者更新记录，返回值true代表是创建，false代表是更新
func (db *ObjectDB) CreateOrUpdate(ctx SQLContext, packageID cdssdk.PackageID, path string, size int64, fileHash string) (cdssdk.ObjectID, bool, error) {
	// 首次上传Object时，默认不启用冗余，即使是在更新一个已有的Object也是如此
	defRed := cdssdk.NewNoneRedundancy()

	sql := "insert into Object(PackageID, Path, Size, FileHash, Redundancy) values(?,?,?,?,?) on duplicate key update Size = ?, FileHash = ?, Redundancy = ?"

	ret, err := ctx.Exec(sql, packageID, path, size, fileHash, defRed, size, fileHash, defRed)
	if err != nil {
		return 0, false, fmt.Errorf("insert object failed, err: %w", err)
	}

	affs, err := ret.RowsAffected()
	if err != nil {
		return 0, false, fmt.Errorf("getting affected rows: %w", err)
	}

	// 影响行数为1时是插入，为2时是更新
	if affs == 1 {
		objectID, err := ret.LastInsertId()
		if err != nil {
			return 0, false, fmt.Errorf("get id of inserted object failed, err: %w", err)
		}
		return cdssdk.ObjectID(objectID), true, nil
	}

	var objID cdssdk.ObjectID
	if err = sqlx.Get(ctx, &objID, "select ObjectID from Object where PackageID = ? and Path = ?", packageID, path); err != nil {
		return 0, false, fmt.Errorf("getting object id: %w", err)
	}

	return objID, false, nil
}

func (*ObjectDB) UpdateFileInfo(ctx SQLContext, objectID cdssdk.ObjectID, fileSize int64) (bool, error) {
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

func (*ObjectDB) GetPackageObjects(ctx SQLContext, packageID cdssdk.PackageID) ([]model.Object, error) {
	var ret []model.TempObject
	err := sqlx.Select(ctx, &ret, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	return lo.Map(ret, func(o model.TempObject, idx int) model.Object { return o.ToObject() }), err
}

func (db *ObjectDB) BatchAdd(ctx SQLContext, packageID cdssdk.PackageID, objs []coormq.AddObjectEntry) ([]cdssdk.ObjectID, error) {
	objIDs := make([]cdssdk.ObjectID, 0, len(objs))
	for _, obj := range objs {
		// 创建对象的记录
		objID, isCreate, err := db.CreateOrUpdate(ctx, packageID, obj.Path, obj.Size, obj.FileHash)
		if err != nil {
			return nil, fmt.Errorf("creating object: %w", err)
		}

		objIDs = append(objIDs, objID)

		if !isCreate {
			// 删除原本所有的编码块记录，重新添加
			if err = db.ObjectBlock().DeleteObjectAll(ctx, objID); err != nil {
				return nil, fmt.Errorf("deleting all object block: %w", err)
			}
		}

		// 首次上传默认使用不分块的rep模式
		err = db.ObjectBlock().Create(ctx, objID, 0, obj.NodeID, obj.FileHash)
		if err != nil {
			return nil, fmt.Errorf("creating object block: %w", err)
		}

		// 创建缓存记录
		err = db.Cache().CreatePinned(ctx, obj.FileHash, obj.NodeID, 0)
		if err != nil {
			return nil, fmt.Errorf("creating cache: %w", err)
		}
	}

	return objIDs, nil
}

func (db *ObjectDB) BatchUpdateRedundancy(ctx SQLContext, objs []coormq.ChangeObjectRedundancyEntry) error {
	for _, obj := range objs {
		_, err := ctx.Exec("update Object set Redundancy = ? where ObjectID = ?", obj.Redundancy, obj.ObjectID)
		if err != nil {
			return fmt.Errorf("updating object: %w", err)
		}

		// 删除原本所有的编码块记录，重新添加
		if err = db.ObjectBlock().DeleteObjectAll(ctx, obj.ObjectID); err != nil {
			return fmt.Errorf("deleting all object block: %w", err)
		}

		for _, block := range obj.Blocks {
			// 首次上传默认使用不分块的rep模式
			err = db.ObjectBlock().Create(ctx, obj.ObjectID, block.Index, block.NodeID, block.FileHash)
			if err != nil {
				return fmt.Errorf("creating object block: %w", err)
			}

			// 创建缓存记录
			err = db.Cache().CreatePinned(ctx, block.FileHash, block.NodeID, 0)
			if err != nil {
				return fmt.Errorf("creating cache: %w", err)
			}
		}
	}

	return nil
}

func (*ObjectDB) BatchDelete(ctx SQLContext, ids []cdssdk.ObjectID) error {
	_, err := ctx.Exec("delete from Object where ObjectID in (?)", ids)
	return err
}

func (*ObjectDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete from Object where PackageID = ?", packageID)
	return err
}
