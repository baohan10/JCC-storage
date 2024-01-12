package db

import (
	"database/sql"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type ObjectBlockDB struct {
	*DB
}

func (db *DB) ObjectBlock() *ObjectBlockDB {
	return &ObjectBlockDB{DB: db}
}

func (db *ObjectBlockDB) GetByNodeID(ctx SQLContext, nodeID cdssdk.NodeID) ([]stgmod.ObjectBlock, error) {
	var rets []stgmod.ObjectBlock
	err := sqlx.Select(ctx, &rets, "select * from ObjectBlock where NodeID = ?", nodeID)
	return rets, err
}

func (db *ObjectBlockDB) Create(ctx SQLContext, objectID cdssdk.ObjectID, index int, nodeID cdssdk.NodeID, fileHash string) error {
	_, err := ctx.Exec("insert into ObjectBlock values(?,?,?,?)", objectID, index, nodeID, fileHash)
	return err
}

func (db *ObjectBlockDB) BatchCreate(ctx SQLContext, blocks []stgmod.ObjectBlock) error {
	_, err := sqlx.NamedExec(ctx,
		"insert ignore into ObjectBlock(ObjectID, `Index`, NodeID, FileHash) values(:ObjectID, :Index, :NodeID, :FileHash)",
		blocks,
	)
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
	query, args, err := sqlx.In("delete from ObjectBlock where NodeID = ? and FileHash in (?)", nodeID, fileHashes)
	if err != nil {
		return err
	}

	_, err = ctx.Exec(query, args...)
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
