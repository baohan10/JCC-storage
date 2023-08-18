package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
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

func (db *ObjectDB) Create(ctx SQLContext, packageID int64, path string, size int64) (int64, error) {
	sql := "insert into Object(PackageID, Path, Size) values(?,?,?)"
	ret, err := ctx.Exec(sql, packageID, path, size)
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
func (db *ObjectDB) CreateOrUpdate(ctx SQLContext, packageID int64, path string, size int64) (int64, bool, error) {
	sql := "insert into Object(PackageID, Path, Size) values(?,?,?) on duplicate key update Size = ?"
	ret, err := ctx.Exec(sql, packageID, path, size, size)
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
		return objectID, true, nil
	}

	var objID int64
	if err = sqlx.Get(ctx, &objID, "select ObjectID from Object where PackageID = ? and Path = ?", packageID, path); err != nil {
		return 0, false, fmt.Errorf("getting object id: %w", err)
	}

	return objID, false, nil
}

func (db *ObjectDB) UpdateRepObject(ctx SQLContext, objectID int64, fileSize int64, nodeIDs []int64, fileHash string) error {
	_, err := db.UpdateFileInfo(ctx, objectID, fileSize)
	if err != nil {
		if err != nil {
			return fmt.Errorf("update rep object failed, err: %w", err)
		}
	}

	objRep, err := db.ObjectRep().GetByID(ctx, objectID)
	if err != nil {
		return fmt.Errorf("get object rep failed, err: %w", err)
	}

	// 如果新文件与旧文件的Hash不同，则需要更新关联的FileHash，重新插入Cache记录
	if objRep.FileHash != fileHash {
		_, err := db.ObjectRep().Update(ctx, objectID, fileHash)
		if err != nil {
			return fmt.Errorf("update rep object file hash failed, err: %w", err)
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

func (*ObjectDB) BatchGetAllEcObjectIDs(ctx SQLContext, start int, count int) ([]int64, error) {
	var ret []int64
	rep := "rep"
	err := sqlx.Select(ctx, &ret, "SELECT ObjectID FROM object where Redundancy != ? limit ?, ?", rep, start, count)
	return ret, err
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

func (*ObjectDB) GetPackageObjects(ctx SQLContext, packageID int64) ([]model.Object, error) {
	var ret []model.Object
	err := sqlx.Select(ctx, &ret, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	return ret, err
}

func (db *ObjectDB) BatchAddRep(ctx SQLContext, packageID int64, objs []coormq.AddRepObjectInfo) ([]int64, error) {
	var objIDs []int64
	for _, obj := range objs {
		// 创建对象的记录
		objID, isCreate, err := db.CreateOrUpdate(ctx, packageID, obj.Path, obj.Size)
		if err != nil {
			return nil, fmt.Errorf("creating object: %w", err)
		}

		objIDs = append(objIDs, objID)

		if isCreate {
			if err := db.createRep(ctx, objID, obj); err != nil {
				return nil, err
			}
		} else {
			if err := db.updateRep(ctx, objID, obj); err != nil {
				return nil, err
			}
		}
	}

	return objIDs, nil
}

func (db *ObjectDB) createRep(ctx SQLContext, objID int64, obj coormq.AddRepObjectInfo) error {
	// 创建对象副本的记录
	if err := db.ObjectRep().Create(ctx, objID, obj.FileHash); err != nil {
		return fmt.Errorf("creating object rep: %w", err)
	}

	// 创建缓存记录
	priority := 0 //优先级暂时设置为0
	for _, nodeID := range obj.NodeIDs {
		if err := db.Cache().CreatePinned(ctx, obj.FileHash, nodeID, priority); err != nil {
			return fmt.Errorf("creating cache: %w", err)
		}
	}

	return nil
}
func (db *ObjectDB) updateRep(ctx SQLContext, objID int64, obj coormq.AddRepObjectInfo) error {
	objRep, err := db.ObjectRep().GetByID(ctx, objID)
	if err != nil {
		return fmt.Errorf("getting object rep: %w", err)
	}

	// 如果新文件与旧文件的Hash不同，则需要更新关联的FileHash，重新插入Cache记录
	if objRep.FileHash != obj.FileHash {
		_, err := db.ObjectRep().Update(ctx, objID, obj.FileHash)
		if err != nil {
			return fmt.Errorf("updating rep object file hash: %w", err)
		}

		for _, nodeID := range obj.NodeIDs {
			if err := db.Cache().CreatePinned(ctx, obj.FileHash, nodeID, 0); err != nil {
				return fmt.Errorf("creating cache: %w", err)
			}
		}

	} else {
		// 如果相同，则只增加Cache中不存在的记录
		cachedNodes, err := db.Cache().GetCachingFileNodes(ctx, obj.FileHash)
		if err != nil {
			return fmt.Errorf("finding caching file nodes: %w", err)
		}

		// 筛选出不在cachedNodes中的id
		newNodeIDs := lo.Filter(obj.NodeIDs, func(id int64, index int) bool {
			return lo.NoneBy(cachedNodes, func(node model.Node) bool {
				return node.NodeID == id
			})
		})
		for _, nodeID := range newNodeIDs {
			if err := db.Cache().CreatePinned(ctx, obj.FileHash, nodeID, 0); err != nil {
				return fmt.Errorf("creating cache: %w", err)
			}
		}
	}

	return nil
}

func (db *ObjectDB) BatchAddEC(ctx SQLContext, packageID int64, objs []coormq.AddECObjectInfo) ([]int64, error) {
	objIDs := make([]int64, 0, len(objs))
	for _, obj := range objs {
		// 创建对象的记录
		objID, isCreate, err := db.CreateOrUpdate(ctx, packageID, obj.Path, obj.Size)
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
		for i := 0; i < len(obj.FileHashes); i++ {
			err := db.ObjectBlock().Create(ctx, objID, i, obj.FileHashes[i])
			if err != nil {
				return nil, fmt.Errorf("creating object block: %w", err)
			}
		}

		// 创建缓存记录
		priority := 0 //优先级暂时设置为0
		for i, nodeID := range obj.NodeIDs {
			err = db.Cache().CreatePinned(ctx, obj.FileHashes[i], nodeID, priority)
			if err != nil {
				return nil, fmt.Errorf("creating cache: %w", err)
			}
		}
	}

	return objIDs, nil
}

func (*ObjectDB) BatchDelete(ctx SQLContext, ids []int64) error {
	_, err := ctx.Exec("delete from Object where ObjectID in (?)", ids)
	return err
}
