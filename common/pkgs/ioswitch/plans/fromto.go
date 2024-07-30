package plans

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type FromTo struct {
	Object stgmod.ObjectDetail
	Froms  []From
	Tos    []To
}

func (ft *FromTo) AddFrom(from From) *FromTo {
	ft.Froms = append(ft.Froms, from)
	return ft
}

func (ft *FromTo) AddTo(to To) *FromTo {
	ft.Tos = append(ft.Tos, to)
	return ft
}

type FromTos []FromTo

type From interface {
	GetDataIndex() int
}

type To interface {
	GetRange() Range
	GetDataIndex() int
}

type Range struct {
	Offset int64
	Length int64
}

type FromExecutor struct {
	Handle    *ExecutorWriteStream
	DataIndex int
}

func (f *FromExecutor) GetDataIndex() int {
	return f.DataIndex
}

func (f *FromExecutor) BuildNode(ft *FromTo) Node {
	op := Node{
		Env: &ExecutorEnv{},
		Type: &FromExecutorOp{
			Handle: f.Handle,
		},
	}
	op.NewOutput(f.DataIndex)
	return op
}

type FromNode struct {
	Node      *cdssdk.Node
	DataIndex int
}

func NewFromNode(node *cdssdk.Node, dataIndex int) *FromNode {
	return &FromNode{
		Node:      node,
		DataIndex: dataIndex,
	}
}

func (f *FromNode) GetDataIndex() int {
	return f.DataIndex
}

type ToExecutor struct {
	Handle    *ExecutorReadStream
	DataIndex int
	Range     Range
}

func NewToExecutor(dataIndex int) (*ToExecutor, *ExecutorReadStream) {
	str := ExecutorReadStream{}
	return &ToExecutor{
		Handle:    &str,
		DataIndex: dataIndex,
	}, &str
}

func (t *ToExecutor) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToExecutor) GetRange() Range {
	return t.Range
}

type ToAgent struct {
	Node             cdssdk.Node
	DataIndex        int
	Range            Range
	FileHashStoreKey string
}

func NewToAgent(node cdssdk.Node, dataIndex int, fileHashStoreKey string) *ToAgent {
	return &ToAgent{
		Node:             node,
		DataIndex:        dataIndex,
		FileHashStoreKey: fileHashStoreKey,
	}
}

func (t *ToAgent) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToAgent) GetRange() Range {
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
