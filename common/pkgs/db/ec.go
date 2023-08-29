package db

import (
	//"database/sql"

	"github.com/jmoiron/sqlx"
	//"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type EcDB struct {
	*DB
}

func (db *DB) Ec() *EcDB {
	return &EcDB{DB: db}
}

// GetEc 查询纠删码参数
func (db *EcDB) GetEc(ctx SQLContext, ecName string) (model.Ec, error) {
	var ret model.Ec
	err := sqlx.Get(ctx, &ret, "select * from Ec where Name = ?", ecName)
	return ret, err
}

func (db *EcDB) GetEcName(ctx SQLContext, objectID int) (string, error) {
	var ret string
	err := sqlx.Get(ctx, &ret, "select Redundancy from Object where ObjectID = ?")
	return ret, err
}
