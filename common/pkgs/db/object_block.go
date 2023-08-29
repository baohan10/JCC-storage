package db

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/storage-common/consts"
	"gitlink.org.cn/cloudream/storage-common/models"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type ObjectBlockDB struct {
	*DB
}

func (db *DB) ObjectBlock() *ObjectBlockDB {
	return &ObjectBlockDB{DB: db}
}

func (db *ObjectBlockDB) Create(ctx SQLContext, objectID int64, index int, fileHash string) error {
	_, err := ctx.Exec("insert into ObjectBlock(ObjectID, Index, FileHash) values(?,?,?)", objectID, index, fileHash)
	return err
}

func (db *ObjectBlockDB) DeleteObjectAll(ctx SQLContext, objectID int64) error {
	_, err := ctx.Exec("delete from ObjectBlock where ObjectID = ?", objectID)
	return err
}

func (db *ObjectBlockDB) DeleteInPackage(ctx SQLContext, packageID int64) error {
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

func (db *ObjectBlockDB) GetBatchObjectBlocks(ctx SQLContext, objectIDs []int64) ([][]string, error) {
	blocks := make([][]string, len(objectIDs))
	var err error
	for i, objectID := range objectIDs {
		var x []model.ObjectBlock
		sql := "select * from ObjectBlock where ObjectID=?"
		err = db.d.Select(&x, sql, objectID)
		xx := make([]string, len(x))
		for ii := 0; ii < len(x); ii++ {
			xx[x[ii].Index] = x[ii].FileHash
		}
		blocks[i] = xx
	}
	return blocks, err
}

func (db *ObjectBlockDB) GetBatchBlocksNodes(ctx SQLContext, hashs [][]string) ([][][]int64, error) {
	nodes := make([][][]int64, len(hashs))
	var err error
	for i, hs := range hashs {
		fileNodes := make([][]int64, len(hs))
		for j, h := range hs {
			var x []model.Node
			err = sqlx.Select(ctx, &x,
				"select Node.* from Cache, Node where"+
					" Cache.FileHash=? and Cache.NodeID = Node.NodeID and Cache.State=?", h, consts.CacheStatePinned)
			xx := make([]int64, len(x))
			for ii := 0; ii < len(x); ii++ {
				xx[ii] = x[ii].NodeID
			}
			fileNodes[j] = xx
		}
		nodes[i] = fileNodes
	}
	return nodes, err
}

func (db *ObjectBlockDB) GetWithNodeIDInPackage(ctx SQLContext, packageID int64) ([]models.ObjectECData, error) {
	var objs []model.Object
	err := sqlx.Select(ctx, &objs, "select * from Object where PackageID = ? order by ObjectID asc", packageID)
	if err != nil {
		return nil, fmt.Errorf("query objectIDs: %w", err)
	}

	rets := make([]models.ObjectECData, 0, len(objs))

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

		blocks := make([]models.ObjectBlockData, 0, len(tmpRets))
		for _, tmp := range tmpRets {
			var block models.ObjectBlockData
			block.Index = tmp.Index
			block.FileHash = tmp.FileHash

			if tmp.NodeIDs != nil {
				block.NodeIDs = splitIDStringUnsafe(*tmp.NodeIDs)
			}

			blocks = append(blocks, block)
		}

		rets = append(rets, models.NewObjectECData(obj, blocks))
	}

	return rets, nil
}
