package db

import (
	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type NodeConnectivityDB struct {
	*DB
}

func (db *DB) NodeConnectivity() *NodeConnectivityDB {
	return &NodeConnectivityDB{DB: db}
}

func (db *NodeConnectivityDB) BatchGetByFromNode(ctx SQLContext, fromNodeIDs []cdssdk.NodeID) ([]model.NodeConnectivity, error) {
	if len(fromNodeIDs) == 0 {
		return nil, nil
	}

	var ret []model.NodeConnectivity

	sql, args, err := sqlx.In("select * from NodeConnectivity where FromNodeID in (?)", fromNodeIDs)
	if err != nil {
		return nil, err
	}

	return ret, sqlx.Select(ctx, &ret, sql, args...)
}

func (db *NodeConnectivityDB) BatchUpdateOrCreate(ctx SQLContext, cons []model.NodeConnectivity) error {
	if len(cons) == 0 {
		return nil
	}

	return BatchNamedExec(ctx,
		"insert into NodeConnectivity(FromNodeID, ToNodeID, Delay, TestTime) values(:FromNodeID, :ToNodeID, :Delay, :TestTime) as new"+
			" on duplicate key update Delay = new.Delay, TestTime = new.TestTime", 4, cons, nil)
}
