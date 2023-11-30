package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type ObjectBlockDB struct {
	*DB
}

func (db *DB) ObjectBlock() *ObjectBlockDB {
	return &ObjectBlockDB{DB: db}
}

func (db *ObjectBlockDB) Create(ctx SQLContext, objectID cdssdk.ObjectID, index int, fileHash string, nodeID cdssdk.NodeID) error {
	_, err := ctx.Exec("insert into ObjectBlock values(?,?,?,?)", objectID, index, fileHash, nodeID)
	return err
}

func (db *ObjectBlockDB) DeleteObjectAll(ctx SQLContext, objectID cdssdk.ObjectID) error {
	_, err := ctx.Exec("delete from ObjectBlock where ObjectID = ?", objectID)
	return err
}

func (db *ObjectBlockDB) DeleteInPackage(ctx SQLContext, packageID cdssdk.PackageID) error {
	_, err := ctx.Exec("delete ObjectBlock from ObjectBlock inner join Object on ObjectBlock.ObjectID = Object.ObjectID where PackageID = ?", packageID)
	return err
}

func (db *ObjectBlockDB) CountBlockWithHash(ctx SQLContext, fileHash string) (int, error) {
	var cnt int
	err := sqlx.Get(ctx, &cnt,
		"select count(FileHash) from ObjectBlock, Object, Package where FileHash = ? and"+
			" ObjectBlock.ObjectID = Object.ObjectID and"+
			" Object.PackageID = Package.PackageID and"+
			" Package.State = ?", fileHash, consts.PackageStateNormal)
	if err == sql.ErrNoRows {
		return 0, nil
	}

	return cnt, err
}

func (db *ObjectBlockDB) GetWithNodeIDInPackage(ctx SQLContext, packageID cdssdk.PackageID) ([]stgmod.ObjectECData, error) {
	var objs []model.Object
	err := sqlx.Select(ctx, &objs, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	if err != nil {
		return nil, fmt.Errorf("query objectIDs: %w", err)
	}

	rets := make([]stgmod.ObjectECData, 0, len(objs))

	for _, obj := range objs {
		var tmpRets []struct {
			Index    int     `db:"Index"`
			FileHash string  `db:"FileHash"`
			NodeIDs  *string `db:"NodeIDs"`
		}

		err := sqlx.Select(ctx,
			&tmpRets,
			"select ObjectBlock.Index, ObjectBlock.FileHash, group_concat(NodeID) as NodeIDs from ObjectBlock"+
				" left join Cache on ObjectBlock.FileHash = Cache.FileHash"+
				" where ObjectID = ? group by ObjectBlock.Index, ObjectBlock.FileHash",
			obj.ObjectID,
		)
		if err != nil {
			return nil, err
		}

		blocks := make([]stgmod.ObjectBlockData, 0, len(tmpRets))
		for _, tmp := range tmpRets {
			var block stgmod.ObjectBlockData
			block.Index = tmp.Index
			block.FileHash = tmp.FileHash

			if tmp.NodeIDs != nil {
				block.CachedNodeIDs = splitIDStringUnsafe(*tmp.NodeIDs)
			}

			blocks = append(blocks, block)
		}

		rets = append(rets, stgmod.NewObjectECData(obj, blocks))
	}

	return rets, nil
}

// 按逗号切割字符串，并将每一个部分解析为一个int64的ID。
// 注：需要外部保证分隔的每一个部分都是正确的10进制数字格式
func splitIDStringUnsafe(idStr string) []cdssdk.NodeID {
	idStrs := strings.Split(idStr, ",")
	ids := make([]cdssdk.NodeID, 0, len(idStrs))

	for _, str := range idStrs {
		// 假设传入的ID是正确的数字格式
		id, _ := strconv.ParseInt(str, 10, 64)
		ids = append(ids, cdssdk.NodeID(id))
	}

	return ids
}
