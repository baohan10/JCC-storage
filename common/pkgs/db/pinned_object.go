package db

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type PinnedObjectDB struct {
	*DB
}

func (db *DB) PinnedObject() *PinnedObjectDB {
	return &PinnedObjectDB{DB: db}
}

func (*PinnedObjectDB) GetByNodeID(ctx SQLContext, nodeID cdssdk.NodeID) ([]cdssdk.PinnedObject, error) {
	var ret []cdssdk.PinnedObject
	err := sqlx.Select(ctx, &ret, "select * from PinnedObject where NodeID = ?", nodeID)
	return ret, err
}

func (*PinnedObjectDB) GetObjectsByNodeID(ctx SQLContext, nodeID cdssdk.NodeID) ([]cdssdk.Object, error) {
	var ret []model.TempObject
	err := sqlx.Select(ctx, &ret, "select Object.* from PinnedObject, Object where PinnedObject.ObjectID = Object.ObjectID and NodeID = ?", nodeID)
	return lo.Map(ret, func(o model.TempObject, idx int) cdssdk.Object { return o.ToObject() }), err
}

func (*PinnedObjectDB) Create(ctx SQLContext, nodeID cdssdk.NodeID, objectID cdssdk.ObjectID, createTime time.Time) error {
	_, err := ctx.Exec("insert into PinnedObject values(?,?,?)", nodeID, objectID, createTime)
	return err
}

func (*PinnedObjectDB) CreateFromPackage(ctx SQLContext, packageID cdssdk.PackageID, nodeID cdssdk.NodeID) error {
	_, err := ctx.Exec(
		"insert ignore into PinnedObject(NodeID, ObjectID, CreateTime) select ? as NodeID, ObjectID, ? as CreateTime from Object where PackageID = ?",
		nodeID,
		time.Now(),
		packageID,
	)
	return err
}

func (*PinnedObjectDB) Delete(ctx SQLContext, nodeID cdssdk.NodeID, objectID cdssdk.ObjectID) error {
	_, err := ctx.Exec("delete from PinnedObject where NodeID = ? and ObjectID = ?", nodeID, objectID)
	return err
}

func (*PinnedObjectDB) DeleteByObjectID(ctx SQLContext, objectID cdssdk.ObjectID) error {
	_, err := ctx.Exec("delete from PinnedObject where ObjectID = ?", objectID)
	return err
}

func (*PinnedObjectDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete PinnedObject from PinnedObject inner join Object on PinnedObject.ObjectID = Object.ObjectID where PackageID = ?", packageID)
	return err
}

func (*PinnedObjectDB) NodeBatchDelete(ctx SQLContext, nodeID cdssdk.NodeID, objectIDs []cdssdk.ObjectID) error {
	query, args, err := sqlx.In("delete from PinnedObject where NodeID = ? and ObjectID in (?)", objectIDs)
	if err != nil {
		return err
	}
	_, err = ctx.Exec(query, args...)
	return err
}
