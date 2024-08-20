package ioswitch

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type From interface {
	GetDataIndex() int
}

type To interface {
	// To所需要的文件流的范围。具体含义与DataIndex有关系：
	// 如果DataIndex == -1，则表示在整个文件的范围。
	// 如果DataIndex >= 0，则表示在文件的某个分片的范围。
	GetRange() exec.Range
	GetDataIndex() int
}

type FromTos []FromTo

type FromTo struct {
	Froms []From
	Toes  []To
}

func NewFromTo() FromTo {
	return FromTo{}
}

func (ft *FromTo) AddFrom(from From) *FromTo {
	ft.Froms = append(ft.Froms, from)
	return ft
}

func (ft *FromTo) AddTo(to To) *FromTo {
	ft.Toes = append(ft.Toes, to)
	return ft
}

type FromDriver struct {
	Handle    *exec.DriverWriteStream
	DataIndex int
}

func NewFromDriver(dataIndex int) (*FromDriver, *exec.DriverWriteStream) {
	handle := &exec.DriverWriteStream{
		RangeHint: &exec.Range{},
	}
	return &FromDriver{
		Handle:    handle,
		DataIndex: dataIndex,
	}, handle
}

func (f *FromDriver) GetDataIndex() int {
	return f.DataIndex
}

type FromNode struct {
	FileHash  string
	Node      *cdssdk.Node
	DataIndex int
}

func NewFromNode(fileHash string, node *cdssdk.Node, dataIndex int) *FromNode {
	return &FromNode{
		FileHash:  fileHash,
		Node:      node,
		DataIndex: dataIndex,
	}
}

func (f *FromNode) GetDataIndex() int {
	return f.DataIndex
}

type ToDriver struct {
	Handle    *exec.DriverReadStream
	DataIndex int
	Range     exec.Range
}

func NewToDriver(dataIndex int) (*ToDriver, *exec.DriverReadStream) {
	str := exec.DriverReadStream{}
	return &ToDriver{
		Handle:    &str,
		DataIndex: dataIndex,
	}, &str
}

func NewToExecutorWithRange(dataIndex int, rng exec.Range) (*ToDriver, *exec.DriverReadStream) {
	str := exec.DriverReadStream{}
	return &ToDriver{
		Handle:    &str,
		DataIndex: dataIndex,
		Range:     rng,
	}, &str
}

func (t *ToDriver) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToDriver) GetRange() exec.Range {
	return t.Range
}

type ToNode struct {
	Node             cdssdk.Node
	DataIndex        int
	Range            exec.Range
	FileHashStoreKey string
}

func NewToNode(node cdssdk.Node, dataIndex int, fileHashStoreKey string) *ToNode {
	return &ToNode{
		Node:             node,
		DataIndex:        dataIndex,
		FileHashStoreKey: fileHashStoreKey,
	}
}

func NewToNodeWithRange(node cdssdk.Node, dataIndex int, fileHashStoreKey string, rng exec.Range) *ToNode {
	return &ToNode{
		Node:             node,
		DataIndex:        dataIndex,
		FileHashStoreKey: fileHashStoreKey,
		Range:            rng,
	}
}

func (t *ToNode) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToNode) GetRange() exec.Range {
	return t.Range
}

// type ToStorage struct {
// 	Storage   cdssdk.Storage
// 	DataIndex int
// }

// func NewToStorage(storage cdssdk.Storage, dataIndex int) *ToStorage {
// 	return &ToStorage{
// 		Storage:   storage,
// 		DataIndex: dataIndex,
// 	}
// }

// func (t *ToStorage) GetDataIndex() int {
// 	return t.DataIndex
// }
