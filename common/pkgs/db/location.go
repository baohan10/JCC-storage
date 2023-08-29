package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type LocationDB struct {
	*DB
}

func (db *DB) Location() *LocationDB {
	return &LocationDB{DB: db}
}

func (*LocationDB) GetByID(ctx SQLContext, id int64) (model.Location, error) {
	var ret model.Location
	err := sqlx.Get(ctx, &ret, "select * from Location where LocationID = ?", id)
	return ret, err
}

func (db *LocationDB) FindLocationByExternalIP(ctx SQLContext, ip string) (model.Location, error) {
	var locID int64
	err := sqlx.Get(ctx, &locID, "select LocationID from Node where ExternalIP = ?", ip)
	if err != nil {
		return model.Location{}, fmt.Errorf("find node by external ip: %w", err)
	}

	return db.GetByID(ctx, locID)
}
