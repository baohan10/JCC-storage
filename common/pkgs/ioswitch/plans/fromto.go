package plans

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type From interface {
	GetDataIndex() int
}

type To interface {
	GetDataIndex() int
}

type FromTos []FromTo

type FromTo struct {
	Froms []From
	Tos   []To
}

type FromExecutor struct {
	Stream    ExecutorWriteStream
	DataIndex int
}

func (f *FromExecutor) GetDataIndex() int {
	return f.DataIndex
}

type FromIPFS struct {
	Node      cdssdk.Node
	FileHash  string
	DataIndex int
}

func NewFromIPFS(node cdssdk.Node, fileHash string, dataIndex int) *FromIPFS {
	return &FromIPFS{
		Node:      node,
		FileHash:  fileHash,
		DataIndex: dataIndex,
	}
}

func (f *FromIPFS) GetDataIndex() int {
	return f.DataIndex
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

type ToStorage struct {
	Storage   cdssdk.Storage
	DataIndex int
}

func NewToStorage(storage cdssdk.Storage, dataIndex int) *ToStorage {
	return &ToStorage{
		Storage:   storage,
		DataIndex: dataIndex,
	}
}

func (t *ToStorage) GetDataIndex() int {
	return t.DataIndex
}
