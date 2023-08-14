package db

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/storage-common/consts"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type ObjectRepDB struct {
	*DB
}

func (db *DB) ObjectRep() *ObjectRepDB {
	return &ObjectRepDB{DB: db}
}

// GetObjectRep 查询对象副本表
func (db *ObjectRepDB) GetByID(ctx SQLContext, objectID int64) (model.ObjectRep, error) {
	var ret model.ObjectRep
	err := sqlx.Get(ctx, &ret, "select * from ObjectRep where ObjectID = ?", objectID)
	return ret, err
}

func (db *ObjectRepDB) UpdateFileHash(ctx SQLContext, objectID int64, fileHash string) (int64, error) {
	ret, err := ctx.Exec("update ObjectRep set FileHash = ? where ObjectID = ?", fileHash, objectID)
	if err != nil {
		return 0, err
	}

	cnt, err := ret.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get affected rows failed, err: %w", err)
	}

	return cnt, nil
}

func (db *ObjectRepDB) Delete(ctx SQLContext, objectID int64) error {
	_, err := ctx.Exec("delete from ObjectRep where ObjectID = ?", objectID)
	return err
}

func (db *ObjectRepDB) GetFileMaxRepCount(ctx SQLContext, fileHash string) (int, error) {
	var maxRepCnt *int
	err := sqlx.Get(ctx, &maxRepCnt,
		"select max(RepCount) from ObjectRep, Object where FileHash = ? and "+
			"ObjectRep.ObjectID = Object.ObjectID and "+
			"Object.State = ?", fileHash, consts.ObjectStateNormal)

	if err == sql.ErrNoRows {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	if maxRepCnt == nil {
		return 0, nil
	}

	return *maxRepCnt, err
}
