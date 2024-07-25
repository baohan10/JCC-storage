package plans

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type From interface {
	GetDataIndex() int
	BuildOp() Node
}

type To interface {
	GetDataIndex() int
	BuildOp() Node
}

type FromTos []FromTo

type FromTo struct {
	Froms []From
	Tos   []To
}

type FromExecutor struct {
	Stream    *ExecutorWriteStream
	DataIndex int
}

func (f *FromExecutor) GetDataIndex() int {
	return f.DataIndex
}

func (f *FromExecutor) BuildOp() Node {
	op := Node{
		Env: &ExecutorEnv{},
		Type: FromExecutorOp{
			OutputVar: 0,
			Handle:    f.Stream,
		},
	}
	op.NewOutput(nil)
	return op
}

type FromIPFS struct {
	Node      *cdssdk.Node
	FileHash  string
	DataIndex int
}

func NewFromIPFS(node *cdssdk.Node, fileHash string, dataIndex int) *FromIPFS {
	return &FromIPFS{
		Node:      node,
		FileHash:  fileHash,
		DataIndex: dataIndex,
	}
}

func (f *FromIPFS) GetDataIndex() int {
	return f.DataIndex
}

func (f *FromIPFS) BuildOp() Node {
	op := Node{
		Pinned: true,
		Type: &IPFSReadType{
			OutputVar: 0,
			FileHash:  f.FileHash,
		},
	}

	if f.Node == nil {
		op.Env = nil
	} else {
		op.Env = &AgentEnv{*f.Node}
	}

	op.NewOutput(nil)
	return op
}

type ToExecutor struct {
	Stream    *ExecutorReadStream
	DataIndex int
}

func NewToExecutor(dataIndex int) (*ToExecutor, *ExecutorReadStream) {
	str := ExecutorReadStream{}
	return &ToExecutor{
		Stream:    &str,
		DataIndex: dataIndex,
	}, &str
}

func (t *ToExecutor) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToExecutor) BuildOp() Node {
	op := Node{
		Env:    &ExecutorEnv{},
		Pinned: true,
		Type: ToExecutorOp{
			InputVar: 0,
			Handle:   t.Stream,
		},
	}
	op.NewOutput(nil)
	return op
}

type ToIPFS struct {
	Node        cdssdk.Node
	DataIndex   int
	FileHashKey string
}

func NewToIPFS(node cdssdk.Node, dataIndex int, fileHashKey string) *ToIPFS {
	return &ToIPFS{
		Node:        node,
		DataIndex:   dataIndex,
		FileHashKey: fileHashKey,
	}
}

func (t *ToIPFS) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToIPFS) BuildOp() Node {
	op := Node{
		Env:    &AgentEnv{t.Node},
		Pinned: true,
		Type: &IPFSWriteType{
			InputVar:    0,
			FileHashVar: 0,
		},
	}
	op.NewInput(nil)
	return op
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
