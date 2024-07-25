package plans

import (
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type VarIndex int

type StreamVar struct {
	DataIndex int
	From      *Node
	Toes      []*Node
	Var       *ioswitch.StreamVar
}

func (v *StreamVar) AddTo(to *Node) {
	v.Toes = append(v.Toes, to)
}

func (v *StreamVar) RemoveTo(to *Node) {
	v.Toes = lo2.Remove(v.Toes, to)
}

type ValueVarType int

const (
	StringValueVar ValueVarType = iota
)

type ValueVar struct {
	Type ValueVarType
	From *Node
	Toes []*Node
	Var  ioswitch.Var
}

func (v *ValueVar) AddTo(to *Node) {
	v.Toes = append(v.Toes, to)
}

func (v *ValueVar) RemoveTo(to *Node) {
	v.Toes = lo2.Remove(v.Toes, to)
}

type OpEnv interface {
	Equals(env OpEnv) bool
}

type AgentEnv struct {
	Node cdssdk.Node
}

func (e *AgentEnv) Equals(env OpEnv) bool {
	if agentEnv, ok := env.(*AgentEnv); ok {
		return e.Node.NodeID == agentEnv.Node.NodeID
	}
	return false
}

type ExecutorEnv struct{}

func (e *ExecutorEnv) Equals(env OpEnv) bool {
	_, ok := env.(*ExecutorEnv)
	return ok
}

type OpType interface {
	GenerateOp(node *Node, blder *PlanBuilder) error
}

type Node struct {
	Env           OpEnv // Op将在哪里执行，Agent或者Executor
	Type          OpType
	InputStreams  []*StreamVar
	OutputStreams []*StreamVar
	InputValues   []*ValueVar
	OutputValues  []*ValueVar
}

func (o *Node) NewOutput(dataIndex int) *StreamVar {
	v := &StreamVar{DataIndex: dataIndex, From: o}
	o.OutputStreams = append(o.OutputStreams, v)
	return v
}

func (o *Node) AddInput(str *StreamVar) {
	o.InputStreams = append(o.InputStreams, str)
	str.AddTo(o)
}

func (o *Node) ReplaceInput(org *StreamVar, new *StreamVar) {
	idx := lo.IndexOf(o.InputStreams, org)
	if idx < 0 {
		return
	}

	o.InputStreams[idx].RemoveTo(o)
	o.InputStreams[idx] = new
	new.AddTo(o)
}

func (o *Node) NewOutputVar(typ ValueVarType) *ValueVar {
	v := &ValueVar{Type: typ, From: o}
	o.OutputValues = append(o.OutputValues, v)
	return v
}

func (o *Node) AddInputVar(v *ValueVar) {
	o.InputValues = append(o.InputValues, v)
	v.AddTo(o)
}

func (o *Node) ReplaceInputVar(org *ValueVar, new *ValueVar) {
	idx := lo.IndexOf(o.InputValues, org)
	if idx < 0 {
		return
	}

	o.InputValues[idx].RemoveTo(o)
	o.InputValues[idx] = new
	new.AddTo(o)
}

type IPFSReadType struct {
	FileHash string
	Option   ipfs.ReadOption
}

func (t *IPFSReadType) GenerateOp(op *Node, blder *PlanBuilder) error {

}

type IPFSWriteType struct {
	FileHashStoreKey string
}

func (t *IPFSWriteType) GenerateOp(op *Node, blder *PlanBuilder) error {

}

type ChunkedSplitOp struct {
	ChunkSize    int
	PaddingZeros bool
}

func (t *ChunkedSplitOp) GenerateOp(op *Node, blder *PlanBuilder) error {

}

type ChunkedJoinOp struct {
	ChunkSize int
}

func (t *ChunkedJoinOp) GenerateOp(op *Node, blder *PlanBuilder) error {

}

type CloneStreamOp struct{}

func (t *CloneStreamOp) GenerateOp(op *Node, blder *PlanBuilder) error {

}

type CloneVarOp struct{}

func (t *CloneVarOp) GenerateOp(op *Node, blder *PlanBuilder) error {

}

type MultiplyOp struct {
	Coef      [][]byte
	ChunkSize int
}

func (t *MultiplyOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type FileReadOp struct {
	FilePath string
}

func (t *FileReadOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type FileWriteOp struct {
	FilePath string
}

func (t *FileWriteOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type FromExecutorOp struct {
	Handle *ExecutorWriteStream
}

func (t *FromExecutorOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type ToExecutorOp struct {
	Handle *ExecutorReadStream
}

func (t *ToExecutorOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type StoreOp struct {
	StoreKey string
}

func (t *StoreOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type DropOp struct{}

func (t *DropOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type SendStreamOp struct{}

func (t *SendStreamOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type GetStreamOp struct{}

func (t *GetStreamOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type SendVarOp struct{}

func (t *SendVarOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}

type GetVarOp struct{}

func (t *GetVarOp) GenerateOp(op *Node, blder *PlanBuilder) error {
}
