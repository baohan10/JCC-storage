package main

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/utils/consts"
)

// 数据库指针
var db *sqlx.DB

// 错误处理函数
func HandleError(why string, err error) {
	if err != nil {
		fmt.Println(why, err)
	}
}

// 初始化数据库连接，init()方法系统会在动在main方法之前执行。
func init() {
	database, err := sqlx.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/kx?charset=utf8mb4&parseTime=true")
	HandleError("open mysql failed,", err)
	db = database
}

// 节点延迟表插入
func Insert_NodeDelay(innodeIP string, outnodeIP []string, delay []int) {
	insSql := "insert into NodeDelay values(?,?,?)"
	updateSql := "UPDATE NodeDelay SET DelayInMs=? WHERE InNodeIP=? AND OutNodeIP=?"
	for i := 0; i < len(outnodeIP); i++ {
		_, err := db.Exec(insSql, innodeIP, outnodeIP[i], delay[i])
		//HandleError("insert failed: ", err)
		if err != nil {
			_, e := db.Exec(updateSql, delay[i], innodeIP, outnodeIP[i])
			HandleError("update failed: ", e)
		}

	}
}

// 节点表插入
func Insert_Node(nodeip string, nodelocation string, ipfsstatus string, localdirstatus string) {
	// 根据NodeIP查询，若不存在则插入，若存在则更新
	//查询
	type Node struct {
		NodeIP string `db:"NodeIP"`
	}
	var x Node
	sql := "select NodeIP from Node where NodeIP=?"
	err := db.Get(&x, sql, nodeip)
	HandleError("Get failed: ", err)
	//local和ipfs同时可达才可达
	// TODO 将status字段改成字符串（枚举值）
	NodeStatus := ipfsstatus == consts.IPFS_STATUS_OK && localdirstatus == consts.LOCAL_DIR_STATUS_OK
	//不存在才插入
	if x == (Node{}) {
		sql := "insert into Node values(?,?,?)"
		_, err := db.Exec(sql, nodeip, nodelocation, NodeStatus)
		HandleError("insert failed: ", err)
	} else {
		//存在则更新
		sql := "update Node set NodeStatus=? where NodeIP=?"
		_, err := db.Exec(sql, NodeStatus, nodeip)
		HandleError("update failed: ", err)
	}
	return
}

// 纠删码对象表插入
func Insert_EcObject(Object_Name string, Bucket_ID int, FileSizeInBytes int64, EcName string) (objectid int64) {
	// 根据objectname和bucketid查询，若不存在则插入，若存在则不操作
	//查询
	type Object struct {
		ObjectID   int64  `db:"ObjectID"`
		ObjectName string `db:"ObjectName"`
		BucketID   int    `db:"BucketID"`
	}
	var x Object
	sql := "select ObjectID, ObjectName, BucketID from Object where ObjectName=? AND BucketID=?"
	err := db.Get(&x, sql, Object_Name, Bucket_ID)
	HandleError("Get failed: ", err)
	//不存在才插入
	if x == (Object{}) {
		sql := "insert into Object(ObjectName, BucketID, FileSizeInBytes, Redundancy, NumRep, EcName) values(?,?,?,?,?,?)"
		r, err := db.Exec(sql, Object_Name, Bucket_ID, FileSizeInBytes, false, "-1", EcName)
		HandleError("insert failed: ", err)
		id, err := r.LastInsertId()
		HandleError("exec failed: ", err)
		objectid = id
	} else {
		objectid = x.ObjectID
	}
	return
}

// 多副本对象表插入
func Insert_RepObject(Object_Name string, Bucket_ID int, FileSizeInBytes int64, NumRep int) (objectid int64) {
	// 根据objectname和bucketid查询，若不存在则插入，若存在则不操作
	//查询
	type Object struct {
		ObjectID   int64  `db:"ObjectID"`
		ObjectName string `db:"ObjectName"`
		BucketID   int    `db:"BucketID"`
	}
	var x Object
	sql := "select ObjectID, ObjectName, BucketID from Object where ObjectName=? AND BucketID=?"
	err := db.Get(&x, sql, Object_Name, Bucket_ID)
	HandleError("Get failed: ", err)
	//不存在才插入
	if x == (Object{}) {
		sql := "insert into Object(ObjectName, BucketID, FileSizeInBytes, Redundancy, NumRep) values(?,?,?,?,?)"
		r, err := db.Exec(sql, Object_Name, Bucket_ID, FileSizeInBytes, true, NumRep)
		HandleError("insert failed: ", err)
		id, err := r.LastInsertId()
		HandleError("exec failed: ", err)
		objectid = id
	} else {
		objectid = x.ObjectID
	}
	return
}

// 对象编码块表插入
func Insert_EcObjectBlock(Object_ID int64, Inner_ID int) {
	// 根据objectID查询，若不存在则插入，若存在则不操作
	//查询
	type Block struct {
		ObjectID int64 `db:"ObjectID"`
	}
	var x []Block
	sql := "select ObjectID from ObjectBlock where ObjectID=? AND InnerID=?"
	err := db.Select(&x, sql, Object_ID, Inner_ID)
	HandleError("select failed: ", err)
	//不存在才插入
	if x == nil {
		sql := "insert into ObjectBlock(ObjectID, InnerID) values(?,?)"
		//执行SQL语句
		_, err := db.Exec(sql, Object_ID, Inner_ID)
		HandleError("insert failed: ", err)
		//查询最后一条用户ID，判断是否插入成功
		// id, err := r.LastInsertId()
		// HandleError("exec failed: ", err)
		// fmt.Println("insert EcObjectBlock succ: ", id)
	}
}

// 对象副本表插入
func Insert_ObjectRep(Object_ID int64) {
	sql := "insert into ObjectRep(ObjectID) values(?)"
	_, err := db.Exec(sql, Object_ID)
	HandleError("insert failed: ", err)
}

// 对象编码块表Echash插入
func Insert_EcHash(Object_ID int, Hashs []string) {
	for i := 0; i < len(Hashs); i++ {
		sql := "update ObjectBlock set BlockHash =? where ObjectID = ? AND InnerID = ?"
		_, err := db.Exec(sql, Hashs[i], Object_ID, i)
		HandleError("insert failed: ", err)
	}
}

// 对象副本表rephash插入
func Insert_RepHash(Object_ID int, Hashs string) {
	sql := "update ObjectRep set RepHash =? where ObjectID = ?"
	_, err := db.Exec(sql, Hashs, Object_ID)
	HandleError("insert failed: ", err)
}

// 缓存表插入
func Insert_Cache(Hashs []string, Ips []string, TempOrPin bool) {
	for i := 0; i < len(Hashs); i++ {
		sql := "insert into Cache values(?,?,?,?)"
		_, err := db.Exec(sql, Hashs[i], Ips[i], TempOrPin, time.Now())
		HandleError("insert failed: ", err)
	}
	return
}

func Query_ObjectID(objectname string) (objectid int) {
	type Object struct {
		ObjectID int `db:"ObjectID"`
	}
	var x Object
	sql := "select ObjectID from Object where ObjectName=? "
	err := db.Get(&x, sql, objectname)
	HandleError("Get failed: ", err)
	if x != (Object{}) {
		objectid = x.ObjectID
	} else {
		fmt.Println("Object not found!")
	}
	// fmt.Println("select bucketid succ:",bucketid)
	return
}

// 根据BucketName查询BucketID
func Query_BucketID(bucketname string) (bucketid int) {
	//桶结构体
	type Bucket struct {
		BucketID   int    `db:"BucketID"`
		BucketName string `db:"BucketName"`
	}
	var x Bucket
	sql := "select BucketID, BucketName from Bucket where BucketName=? "
	err := db.Get(&x, sql, bucketname)
	HandleError("Get failed: ", err)
	if x != (Bucket{}) {
		bucketid = x.BucketID
	} else {
		fmt.Println("Bucket not found!")
		bucketid = -1
	}
	// fmt.Println("select bucketid succ:",bucketid)
	return
}

// 根据用户id查询可用nodeip
func Query_UserNode(user_id int) []string {
	//用户节点结构体
	type UserNode struct {
		UserID int    `db:"UserID"`
		NodeIP string `db:"NodeIP"`
	}
	var x []UserNode
	var node_ip []string
	sql := "select UserID, NodeIP from UserNode where UserID=? "
	err := db.Select(&x, sql, user_id)
	HandleError("select failed: ", err)
	for _, value := range x {
		node_ip = append(node_ip, value.NodeIP)
	}
	fmt.Println("select node_ip succ:", node_ip)
	return node_ip
}

// 根据objectname和bucketid查询对象表,获得redundancy，EcName,fileSizeInBytes
func Query_Object(objectname string, bucketid int) (objectid int, filesizeinbytes int64, redundancy bool, ecname string) {
	//对象结构体
	type Object struct {
		ObjectID        int    `db:"ObjectID"`
		FileSizeInBytes int64  `db:"FileSizeInBytes"`
		Redundancy      bool   `db:"Redundancy"`
		EcName          string `db:"EcName"`
	}
	var x Object
	sql := "select ObjectID, FileSizeInBytes, Redundancy, EcName from Object where ObjectName=? AND BucketID=?"
	err := db.Get(&x, sql, objectname, bucketid)
	HandleError("Get failed: ", err)
	if x != (Object{}) {
		objectid = x.ObjectID
		filesizeinbytes = x.FileSizeInBytes
		redundancy = x.Redundancy
		ecname = x.EcName
	} else {
		fmt.Println("Object not found!")
	}
	return
}

// 查询对象副本表
func Query_ObjectRep(objectid int) (repHash string) {
	//对象结构体
	type ObjectRep struct {
		RepHash string `db:"RepHash"`
	}
	var x ObjectRep
	sql := "select RepHash from ObjectRep where ObjectID=?"
	err := db.Get(&x, sql, objectid)
	HandleError("Get failed: ", err)
	if x != (ObjectRep{}) {
		repHash = x.RepHash
	} else {
		fmt.Println("ObjectRep not found!")
	}
	return
}

// 对象编码块结构体
type ObjectBlock struct {
	InnerID   int    `db:"InnerID"`
	BlockHash string `db:"BlockHash"`
}

// 查询对象编码块表
func Query_ObjectBlock(Object_ID int) (x []ObjectBlock) {
	sql := "select InnerID, BlockHash from ObjectBlock where ObjectID=?"
	err := db.Select(&x, sql, Object_ID)
	HandleError("select failed: ", err)
	return
}

// 将时间字符串转化为时间戳格式（s）
func Time_trans(time_string string) (timestamp int64) {
	timeTemplate1 := "2006-01-02 15:04:05"
	stamp, _ := time.ParseInLocation(timeTemplate1, time_string, time.Local) //使用parseInLocation将字符串格式化返回本地时区时间
	timestamp = stamp.Unix()
	return
}

// 缓存结构体
type Cache struct {
	NodeIP    string `db:"NodeIP"`
	TempOrPin bool   `db:"TempOrPin"`
	Cachetime string `db:"Cachetime"`
}

// 查询缓存表
func Query_Cache(BlockHash string) (x []Cache) {
	sql := "select NodeIP, TempOrPin, Cachetime from Cache where HashValue=?"
	err := db.Select(&x, sql, BlockHash)
	HandleError("Get failed: ", err)
	return
}

// 更新缓存表
func Update_Cache(BlockHash string, nodeip string) (x Cache) {
	//根据hash和nodeip查询缓存表里是否存在此条记录
	sql := "select NodeIP, TempOrPin, Cachetime from Cache where HashValue=? AND NodeIP=?"
	err := db.Select(&x, sql, BlockHash, nodeip)
	HandleError("Get failed: ", err)
	//若在表中已存在且所对应的TempOrPin字段为true，则更新Time
	if x.TempOrPin == true {
		sql = "update Cache set Cachetime=? where HashValue=? AND NodeIP=?"
		_, err := db.Exec(sql, time.Now(), BlockHash, nodeip)
		HandleError("update failed: ", err)
	}
	return
}

// 查询节点延迟表
func Query_NodeDelay(innodeip string, outnodeip string) (delay int) {
	//节点延迟结构体
	type NodeDelay struct {
		DelayInMs int `db:"DelayInMs"`
	}
	var x NodeDelay
	sql := "select DelayInMs from NodeDelay where InNodeIP=? AND OutNodeIP=?"
	err := db.Get(&x, sql, innodeip, outnodeip)
	HandleError("Get failed: ", err)
	if x != (NodeDelay{}) {
		delay = x.DelayInMs
	} else {
		fmt.Println("NodeDelay not found!")
	}
	return
}

/*
func main(){
	// Insert_RepHash(1,"aaa")
	// Hashs := []string{"aaa","bbb","ddd"}
	// // ObjectId := Query_ObjectID(ObjectName)
	// // Insert_EcHash(ObjectId, Hashs)
	// // fmt.Println(BlockID[0])
	// Ips := []string{"chengdu","beijing","changsha"}
	// Insert_Cache(Hashs, Ips, false)

	// var BucketName string = "bucket01"
	// var ObjectName string = "bucket01"
	// BucketID := Query_BucketID(BucketName)
	// ObjectID, fileSizeInBytes, redundancy, EcName := Query_Object(ObjectName, BucketID)
	// fmt.Println(ObjectID,fileSizeInBytes,redundancy,EcName)
	// // repHash := Query_ObjectRep(ObjectID)
    // // fmt.Println(repHash)
	// if redundancy == false{
	// 	objectblock := Query_ObjectBlock(ObjectID)
	// 	var Destination string = "shanghai"
	// 	for _,value := range objectblock{
	// 		Cache := Query_Cache(value.BlockHash)
	// 		Delay := make(map[string]int) // 延迟集合
	// 		for i:=0; i<len(Cache); i++{
	// 			Delay[Cache[i].NodeIP] = Query_NodeDelay(Destination, Cache[i].NodeIP)
	// 		}
	// 		fmt.Println(len(Delay))
	// 		fmt.Println(value.InnerID, value.BlockHash, Cache[0].NodeIP, Cache[0].TempOrPin, Time_trans(Cache[0].Cachetime))
	// 		fmt.Println(value.InnerID, value.BlockHash, Cache[1].NodeIP, Cache[1].TempOrPin, Time_trans(Cache[1].Cachetime))
	// 	}
	// }

	// var ecN int = 5
	// for i:=0; i<ecN; i++{
	// 	Insert_EcObjectBlock(1, i)
	// }

	// fmt.Println(Query_BucketID("bucket01"))

	// a,b,d := "bucket01",123456,"Ec01"
	// var c int64 = 12345678987654321
	// fmt.Println(Insert_EcObject(a, b, c, d))

	// var s int
	// s = 123
	// c := Query_UserNode(s)
	// fmt.Println(c)
}*/
