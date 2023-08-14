package db

import (
	"time"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type NodeDB struct {
	*DB
}

func (db *DB) Node() *NodeDB {
	return &NodeDB{DB: db}
}

func (db *NodeDB) GetByID(ctx SQLContext, nodeID int64) (model.Node, error) {
	var ret model.Node
	err := sqlx.Get(ctx, &ret, "select * from Node where NodeID = ?", nodeID)
	return ret, err
}

func (db *NodeDB) GetAllNodes(ctx SQLContext) ([]model.Node, error) {
	var ret []model.Node
	err := sqlx.Select(ctx, &ret, "select * from Node")
	return ret, err
}

// GetByExternalIP 根据外网IP查找节点
func (db *NodeDB) GetByExternalIP(ctx SQLContext, exterIP string) (model.Node, error) {
	var ret model.Node
	err := sqlx.Get(ctx, &ret, "select * from Node where ExternalIP = ?", exterIP)
	return ret, err
}

// GetUserNodes 根据用户id查询可用node
func (db *NodeDB) GetUserNodes(ctx SQLContext, userID int64) ([]model.Node, error) {
	var nodes []model.Node
	err := sqlx.Select(ctx, &nodes, "select Node.* from UserNode, Node where UserNode.NodeID = Node.NodeID and UserNode.UserID=?", userID)
	return nodes, err
}

// UpdateState 更新状态，并且设置上次上报时间为现在
func (db *NodeDB) UpdateState(ctx SQLContext, nodeID int64, state string) error {
	_, err := ctx.Exec("update Node set State = ?, LastReportTime = ? where NodeID = ?", state, time.Now(), nodeID)
	return err
}
