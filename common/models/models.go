package stgmod

import "gitlink.org.cn/cloudream/storage/common/pkgs/db/model"

/// TODO 将分散在各处的公共结构体定义集中到这里来

type RedundancyData interface{}
type RedundancyDataConst interface {
	RepRedundancyData | ECRedundancyData | RedundancyData
}
type RepRedundancyData struct {
	FileHash string `json:"fileHash"`
}

func NewRedundancyRepData(fileHash string) RepRedundancyData {
	return RepRedundancyData{
		FileHash: fileHash,
	}
}

type ECRedundancyData struct {
	Ec     EC                `json:"ec"`
	Blocks []ObjectBlockData `json:"blocks"`
}

func NewRedundancyEcData(ec EC, blocks []ObjectBlockData) ECRedundancyData {
	return ECRedundancyData{
		Ec:     ec,
		Blocks: blocks,
	}
}

type EC struct {
	ID        int64 `json:"id"`
	K         int   `json:"k"`
	N         int   `json:"n"`
	ChunkSize int   `json:"chunkSize"`
}

type ObjectBlockData struct {
	Index    int     `json:"index"`
	FileHash string  `json:"fileHash"`
	NodeIDs  []int64 `json:"nodeIDs"`
}

func NewObjectBlockData(index int, fileHash string, nodeIDs []int64) ObjectBlockData {
	return ObjectBlockData{
		Index:    index,
		FileHash: fileHash,
		NodeIDs:  nodeIDs,
	}
}

func NewEc(id int64, k int, n int, chunkSize int) EC {
	return EC{
		ID:        id,
		K:         k,
		N:         n,
		ChunkSize: chunkSize,
	}
}

type ObjectRepData struct {
	Object   model.Object `json:"object"`
	FileHash string       `json:"fileHash"`
	NodeIDs  []int64      `json:"nodeIDs"`
}

func NewObjectRepData(object model.Object, fileHash string, nodeIDs []int64) ObjectRepData {
	return ObjectRepData{
		Object:   object,
		FileHash: fileHash,
		NodeIDs:  nodeIDs,
	}
}

type ObjectECData struct {
	Object model.Object      `json:"object"`
	Blocks []ObjectBlockData `json:"blocks"`
}

func NewObjectECData(object model.Object, blocks []ObjectBlockData) ObjectECData {
	return ObjectECData{
		Object: object,
		Blocks: blocks,
	}
}

type LocalMachineInfo struct {
	NodeID     *int64 `json:"nodeID"`
	ExternalIP string `json:"externalIP"`
	LocalIP    string `json:"localIP"`
}
