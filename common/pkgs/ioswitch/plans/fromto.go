package plans

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/math2"
)

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

type FromTos []FromTo

type From interface {
	GetDataIndex() int
}

type To interface {
	// To所需要的文件流的范围。具体含义与DataIndex有关系：
	// 如果DataIndex == -1，则表示在整个文件的范围。
	// 如果DataIndex >= 0，则表示在文件的某个分片的范围。
	GetRange() Range
	GetDataIndex() int
}

type Range struct {
	Offset int64
	Length *int64
}

func (r *Range) Extend(other Range) {
	newOffset := math2.Min(r.Offset, other.Offset)

	if r.Length == nil {
		r.Offset = newOffset
		return
	}

	if other.Length == nil {
		r.Offset = newOffset
		r.Length = nil
		return
	}

	otherEnd := other.Offset + *other.Length
	rEnd := r.Offset + *r.Length

	newEnd := math2.Max(otherEnd, rEnd)
	r.Offset = newOffset
	*r.Length = newEnd - newOffset
}

func (r *Range) ExtendStart(start int64) {
	r.Offset = math2.Min(r.Offset, start)
}

func (r *Range) ExtendEnd(end int64) {
	if r.Length == nil {
		return
	}

	rEnd := r.Offset + *r.Length
	newLen := math2.Max(end, rEnd) - r.Offset
	r.Length = &newLen
}

func (r *Range) Fix(maxLength int64) {
	if r.Length != nil {
		return
	}

	len := maxLength - r.Offset
	r.Length = &len
}

func (r *Range) ToStartEnd(maxLen int64) (start int64, end int64) {
	if r.Length == nil {
		return r.Offset, maxLen
	}

	end = r.Offset + *r.Length
	return r.Offset, end
}

func (r *Range) ClampLength(maxLen int64) {
	if r.Length == nil {
		return
	}

	*r.Length = math2.Min(*r.Length, maxLen-r.Offset)
}

type FromExecutor struct {
	Handle    *ExecutorWriteStream
	DataIndex int
}

func NewFromExecutor(dataIndex int) (*FromExecutor, *ExecutorWriteStream) {
	handle := &ExecutorWriteStream{
		RangeHint: &Range{},
	}
	return &FromExecutor{
		Handle:    handle,
		DataIndex: dataIndex,
	}, handle
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

func NewToExecutorWithRange(dataIndex int, rng Range) (*ToExecutor, *ExecutorReadStream) {
	str := ExecutorReadStream{}
	return &ToExecutor{
		Handle:    &str,
		DataIndex: dataIndex,
		Range:     rng,
	}, &str
}

func (t *ToExecutor) GetDataIndex() int {
	return t.DataIndex
}

func (t *ToExecutor) GetRange() Range {
	return t.Range
}

type ToNode struct {
	Node             cdssdk.Node
	DataIndex        int
	Range            Range
	FileHashStoreKey string
}

func NewToNode(node cdssdk.Node, dataIndex int, fileHashStoreKey string) *ToNode {
	return &ToNode{
		Node:             node,
		DataIndex:        dataIndex,
		FileHashStoreKey: fileHashStoreKey,
	}
}

func NewToNodeWithRange(node cdssdk.Node, dataIndex int, fileHashStoreKey string, rng Range) *ToNode {
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

func (t *ToNode) GetRange() Range {
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
