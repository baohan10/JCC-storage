package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type ObjectBlockDB struct {
	*DB
}

func (db *DB) ObjectBlock() *ObjectBlockDB {
	return &ObjectBlockDB{DB: db}
}

func (db *ObjectBlockDB) GetByNodeID(ctx SQLContext, nodeID cdssdk.NodeID) ([]stgmod.ObjectBlock, error) {
	var rets []stgmod.ObjectBlock
	_, err := ctx.Exec("select * from ObjectBlock where NodeID = ?", nodeID)
	return rets, err
}

func (db *ObjectBlockDB) Create(ctx SQLContext, objectID cdssdk.ObjectID, index int, nodeID cdssdk.NodeID, fileHash string) error {
	_, err := ctx.Exec("insert into ObjectBlock values(?,?,?,?)", objectID, index, nodeID, fileHash)
	return err
}

func (db *ObjectBlockDB) DeleteByObjectID(ctx SQLContext, objectID cdssdk.ObjectID) error {
	_, err := ctx.Exec("delete from ObjectBlock where ObjectID = ?", objectID)
	return err
}

func (db *ObjectBlockDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete ObjectBlock from ObjectBlock inner join Object on ObjectBlock.ObjectID = Object.ObjectID where PackageID = ?", packageID)
	return err
}

func (db *ObjectBlockDB) NodeBatchDelete(ctx SQLContext, nodeID cdssdk.NodeID, fileHashes []string) error {
	_, err := ctx.Exec("delete from ObjectBlock where NodeID = ? and FileHash in (?)", nodeID, fileHashes)
	return err
}

func (db *ObjectBlockDB) CountBlockWithHash(ctx SQLContext, fileHash string) (int, error) {
	var cnt int
	err := sqlx.Get(ctx, &cnt,
		"select count(FileHash) from ObjectBlock, Object, Package where FileHash = ? and"+
			" ObjectBlock.ObjectID = Object.ObjectID and"+
			" Object.PackageID = Package.PackageID and"+
			" Package.State = ?", fileHash, cdssdk.PackageStateNormal)
	if err == sql.ErrNoRows {
		return 0, nil
	}

	return cnt, err
}

func (db *ObjectBlockDB) GetPackageBlockDetails(ctx SQLContext, packageID cdssdk.PackageID) ([]stgmod.ObjectDetail, error) {
	var objs []model.TempObject
	err := sqlx.Select(ctx, &objs, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	if err != nil {
		return nil, fmt.Errorf("getting objects: %w", err)
	}

	rets := make([]stgmod.ObjectDetail, 0, len(objs))

	for _, obj := range objs {
		var cachedObjectNodeIDs []cdssdk.NodeID
		err := sqlx.Select(ctx, &cachedObjectNodeIDs,
			"select NodeID from Object, Cache where"+
				" ObjectID = ? and Object.FileHash = Cache.FileHash",
			obj.ObjectID,
		)
		if err != nil {
			return nil, err
		}

		var blockTmpRets []struct {
			Index         int     `db:"Index"`
			FileHashes    string  `db:"FileHashes"`
			NodeIDs       string  `db:"NodeIDs"`
			CachedNodeIDs *string `db:"CachedNodeIDs"`
		}

		err = sqlx.Select(ctx,
			&blockTmpRets,
			"select ObjectBlock.Index, group_concat(distinct ObjectBlock.FileHash) as FileHashes, group_concat(distinct ObjectBlock.NodeID) as NodeIDs, group_concat(distinct Cache.NodeID) as CachedNodeIDs"+
				" from ObjectBlock left join Cache on ObjectBlock.FileHash = Cache.FileHash"+
				" where ObjectID = ? group by ObjectBlock.Index",
			obj.ObjectID,
		)
		if err != nil {
			return nil, err
		}

		blocks := make([]stgmod.ObjectBlockDetail, 0, len(blockTmpRets))
		for _, tmp := range blockTmpRets {
			var block stgmod.ObjectBlockDetail
			block.Index = tmp.Index

			block.FileHash = splitConcatedFileHash(tmp.FileHashes)[0]
			block.NodeIDs = splitConcatedNodeID(tmp.NodeIDs)
			if tmp.CachedNodeIDs != nil {
				block.CachedNodeIDs = splitConcatedNodeID(*tmp.CachedNodeIDs)
			}

			blocks = append(blocks, block)
		}

		rets = append(rets, stgmod.NewObjectDetail(obj.ToObject(), cachedObjectNodeIDs, blocks))
	}

	return rets, nil
}

// 按逗号切割字符串，并将每一个部分解析为一个int64的ID。
// 注：需要外部保证分隔的每一个部分都是正确的10进制数字格式
func splitConcatedNodeID(idStr string) []cdssdk.NodeID {
	idStrs := strings.Split(idStr, ",")
	ids := make([]cdssdk.NodeID, 0, len(idStrs))

	for _, str := range idStrs {
		// 假设传入的ID是正确的数字格式
		id, _ := strconv.ParseInt(str, 10, 64)
		ids = append(ids, cdssdk.NodeID(id))
	}

	return ids
}

// 按逗号切割字符串
func splitConcatedFileHash(idStr string) []string {
	idStrs := strings.Split(idStr, ",")
	return idStrs
}
