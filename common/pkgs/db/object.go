package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
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
	var ret model.Object
	err := sqlx.Get(ctx, &ret, "select * from Object where ObjectID = ?", objectID)
	return ret, err
}

func (db *ObjectDB) Create(ctx SQLContext, packageID cdssdk.PackageID, path string, size int64, redundancy cdssdk.Redundancy) (int64, error) {
	sql := "insert into Object(PackageID, Path, Size, Redundancy) values(?,?,?,?)"

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
func (db *ObjectDB) CreateOrUpdate(ctx SQLContext, packageID cdssdk.PackageID, path string, size int64, redundancy cdssdk.Redundancy) (cdssdk.ObjectID, bool, error) {
	sql := "insert into Object(PackageID, Path, Size, Redundancy) values(?,?,?,?) on duplicate key update Size = ?, Redundancy = ?"

	ret, err := ctx.Exec(sql, packageID, path, size, redundancy, size, redundancy)
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
	var ret []model.Object
	err := sqlx.Select(ctx, &ret, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	return ret, err
}

func (db *ObjectDB) BatchAdd(ctx SQLContext, packageID cdssdk.PackageID, objs []coormq.AddObjectInfo) ([]cdssdk.ObjectID, error) {
	objIDs := make([]cdssdk.ObjectID, 0, len(objs))
	for _, obj := range objs {
		// 创建对象的记录
		objID, isCreate, err := db.CreateOrUpdate(ctx, packageID, obj.Path, obj.Size, obj.Redundancy)
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

		// 创建编码块的记录
		for _, b := range obj.Blocks {
			for _, n := range b.NodeIDs {
				err := db.ObjectBlock().Create(ctx, objID, b.Index, b.FileHash, n)
				if err != nil {
					return nil, fmt.Errorf("creating object block: %w", err)
				}
			}

			// 创建缓存记录
			priority := 0 //优先级暂时设置为0
			for _, nodeID := range b.CachedNodeIDs {
				err = db.Cache().CreatePinned(ctx, b.FileHash, nodeID, priority)
				if err != nil {
					return nil, fmt.Errorf("creating cache: %w", err)
				}
			}
		}
	}

	return objIDs, nil
}

func (*ObjectDB) BatchDelete(ctx SQLContext, ids []cdssdk.ObjectID) error {
	_, err := ctx.Exec("delete from Object where ObjectID in (?)", ids)
	return err
}

func (*ObjectDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete from Object where PackageID = ?", packageID)
	return err
}
