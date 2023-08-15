package message

import (
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
	mymodels "gitlink.org.cn/cloudream/storage-common/models"
)

type Node struct {
	ID         int64  `json:"id"`
	ExternalIP string `json:"externalIP"`
	LocalIP    string `json:"localIP"`
}

func NewNode(id int64, externalIP string, localIP string) Node {
	return Node{
		ID:         id,
		ExternalIP: externalIP,
		LocalIP:    localIP,
	}
}

type RespNode struct {
	Node
	IsSameLocation bool `json:"isSameLocation"` // 客户端是否与此节点在同一个地域
}

func NewRespNode(id int64, externalIP string, localIP string, isSameLocation bool) RespNode {
	return RespNode{
		Node: Node{
			ID:         id,
			ExternalIP: externalIP,
			LocalIP:    localIP,
		},
		IsSameLocation: isSameLocation,
	}
}

// Resp开头的RedundancyData与RedundancyData的区别在于，多了Nodes等字段。需要一个更好的名称。
type RespRedundancyDataConst interface {
	RespRepRedundancyData | RespEcRedundancyData
}

type RespRedundancyData interface{}

type RespRepRedundancyData struct {
	mymodels.RepRedundancyData
	Nodes []RespNode `json:"nodes"`
}

func NewRespRepRedundancyData(fileHash string, nodes []RespNode) RespRepRedundancyData {
	return RespRepRedundancyData{
		RepRedundancyData: mymodels.RepRedundancyData{
			FileHash: fileHash,
		},
		Nodes: nodes,
	}
}

type RespEcRedundancyData struct {
	Ec     Ec                `json:"ec"`
	Nodes  [][]RespNode      `json:"nodes"`
	Blocks []RespObjectBlock `json:"blocks"`
}

func NewRespEcRedundancyData(ec Ec, blocks []RespObjectBlock, nodes [][]RespNode) RespEcRedundancyData {
	return RespEcRedundancyData{
		Ec:     ec,
		Nodes:  nodes,
		Blocks: blocks,
	}
}

type RespObjectBlock struct {
	mymodels.ObjectBlock
	//Node RespNode `json:"node"`
}

// func NewRespObjectBlock(index int, fileHash string, node RespNode) RespObjectBlock {
func NewRespObjectBlock(index int, fileHash string) RespObjectBlock {
	return RespObjectBlock{
		ObjectBlock: mymodels.ObjectBlock{
			Index:    index,
			FileHash: fileHash,
		},
		//Node: node,
	}
}

type Ec struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	EcK  int    `json:"ecK"`
	EcN  int    `json:"ecN"`
}

func NewEc(id int, name string, k int, n int) Ec {
	return Ec{
		ID:   id,
		Name: name,
		EcK:  k,
		EcN:  n,
	}
}

func init() {
	mq.RegisterTypeSet[models.RedundancyInfo](myreflect.TypeOf[models.RepRedundancyInfo](), myreflect.TypeOf[models.ECRedundancyInfo]())

	mq.RegisterTypeSet[mymodels.RedundancyData](myreflect.TypeOf[mymodels.RepRedundancyData](), myreflect.TypeOf[mymodels.ECRedundancyData]())

	mq.RegisterTypeSet[RespRedundancyData](myreflect.TypeOf[RespRepRedundancyData](), myreflect.TypeOf[RespEcRedundancyData]())
}
