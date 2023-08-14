package models

/// TODO 将分散在各处的公共结构体定义集中到这里来

type RedundancyDataTypes interface{}
type RedundancyDataTypesConst interface {
	RepRedundancyData | ECRedundancyData
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
	Ec     EC            `json:"ec"`
	Blocks []ObjectBlock `json:"blocks"`
}

func NewRedundancyEcData(ec EC, blocks []ObjectBlock) ECRedundancyData {
	return ECRedundancyData{
		Ec:     ec,
		Blocks: blocks,
	}
}

type EC struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	EcK  int    `json:"ecK"`
	EcN  int    `json:"ecN"`
}

type ObjectBlock struct {
	Index    int    `json:"index"`
	FileHash string `json:"fileHash"`
}

func NewObjectBlock(index int, fileHash string) ObjectBlock {
	return ObjectBlock{
		Index:    index,
		FileHash: fileHash,
	}
}

func NewEc(id int, name string, ecK int, ecN int) EC {
	return EC{
		ID:   id,
		Name: name,
		EcK:  ecK,
		EcN:  ecN,
	}
}
