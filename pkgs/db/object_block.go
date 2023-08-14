package db

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/storage-common/consts"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type ObjectBlockDB struct {
	*DB
}

func (db *DB) ObjectBlock() *ObjectBlockDB {
	return &ObjectBlockDB{DB: db}
}

func (db *ObjectBlockDB) Delete(ctx SQLContext, objectID int64) error {
	_, err := ctx.Exec("delete from ObjectBlock where ObjectID = ?", objectID)
	return err
}

func (db *ObjectBlockDB) CountBlockWithHash(ctx SQLContext, fileHash string) (int, error) {
	var cnt int
	err := sqlx.Get(ctx, &cnt,
		"select count(BlockHash) from ObjectBlock, Object where BlockHash = ? and "+
			"ObjectBlock.ObjectID = Object.ObjectID and "+
			"Object.State = ?", fileHash, consts.ObjectStateNormal)
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
			xx[x[ii].InnerID] = x[ii].BlockHash
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
				"select Node.* from Cache, Node where "+
					"Cache.FileHash=? and Cache.NodeID = Node.NodeID and Cache.State=?", h, consts.CacheStatePinned)
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
