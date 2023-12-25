package db

import (
	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type UserDB struct {
	*DB
}

func (db *DB) User() *UserDB {
	return &UserDB{DB: db}
}

func (db *UserDB) GetByID(ctx SQLContext, userID cdssdk.UserID) (model.User, error) {
	var ret model.User
	err := sqlx.Get(ctx, &ret, "select * from User where UserID = ?", userID)
	return ret, err
}
