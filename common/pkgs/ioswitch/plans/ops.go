package plans

import (
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"
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
	SignalValueVar
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

func (o *Node) NewOutputStream(dataIndex int) *StreamVar {
	v := &StreamVar{DataIndex: dataIndex, From: o}
	o.OutputStreams = append(o.OutputStreams, v)
	return v
}

func (o *Node) AddInputStream(str *StreamVar) {
	o.InputStreams = append(o.InputStreams, str)
	str.AddTo(o)
}

func (o *Node) ReplaceInputStream(old *StreamVar, new *StreamVar) {
	old.RemoveTo(o)
	new.AddTo(o)

	idx := lo.IndexOf(o.InputStreams, old)
	o.InputStreams[idx] = new
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

func (o *Node) ReplaceInputVar(old *ValueVar, new *ValueVar) {
	old.RemoveTo(o)
	new.AddTo(o)

	idx := lo.IndexOf(o.InputValues, old)
	o.InputValues[idx] = new
}

func (o *Node) String() string {
	return fmt.Sprintf("Node(%T)", o.Type)
}

type IPFSReadType struct {
	FileHash string
	Option   ipfs.ReadOption
}

func (t *IPFSReadType) GenerateOp(node *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.IPFSRead{
		Output:   node.OutputStreams[0].Var,
		FileHash: t.FileHash,
		Option:   t.Option,
	}, node.Env, blder)
	return nil
}

type IPFSWriteType struct {
	FileHashStoreKey string
	Range            Range
}

func (t *IPFSWriteType) GenerateOp(op *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.IPFSWrite{
		Input:    op.InputStreams[0].Var,
		FileHash: op.OutputValues[0].Var.(*ioswitch.StringVar),
	}, op.Env, blder)
	return nil
}

type ChunkedSplitType struct {
	ChunkSize    int
	PaddingZeros bool
}

func (t *ChunkedSplitType) GenerateOp(op *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.ChunkedSplit{
		Input: op.InputStreams[0].Var,
		Outputs: lo.Map(op.OutputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar {
			return v.Var
		}),
		ChunkSize:    t.ChunkSize,
		PaddingZeros: t.PaddingZeros,
	}, op.Env, blder)
	return nil
}

type ChunkedJoinType struct {
	ChunkSize int
}

func (t *ChunkedJoinType) GenerateOp(op *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.ChunkedJoin{
		Inputs: lo.Map(op.InputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar {
			return v.Var
		}),
		Output:    op.OutputStreams[0].Var,
		ChunkSize: t.ChunkSize,
	}, op.Env, blder)
	return nil
}

type CloneStreamType struct{}

func (t *CloneStreamType) GenerateOp(op *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.CloneStream{
		Input: op.InputStreams[0].Var,
		Outputs: lo.Map(op.OutputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar {
			return v.Var
		}),
	}, op.Env, blder)
	return nil
}

type CloneVarType struct{}

func (t *CloneVarType) GenerateOp(op *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.CloneVar{
		Raw: op.InputValues[0].Var,
		Cloneds: lo.Map(op.OutputValues, func(v *ValueVar, idx int) ioswitch.Var {
			return v.Var
		}),
	}, op.Env, blder)
	return nil
}

type MultiplyOp struct {
	EC cdssdk.ECRedundancy
}

func (t *MultiplyOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	var inputIdxs []int
	var outputIdxs []int
	for _, in := range op.InputStreams {
		inputIdxs = append(inputIdxs, in.DataIndex)
	}
	for _, out := range op.OutputStreams {
		outputIdxs = append(outputIdxs, out.DataIndex)
	}

	rs, err := ec.NewRs(t.EC.K, t.EC.N)
	coef, err := rs.GenerateMatrix(inputIdxs, outputIdxs)
	if err != nil {
		return err
	}

	addOpByEnv(&ops.ECMultiply{
		Coef:      coef,
		Inputs:    lo.Map(op.InputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar { return v.Var }),
		Outputs:   lo.Map(op.OutputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar { return v.Var }),
		ChunkSize: t.EC.ChunkSize,
	}, op.Env, blder)
	return nil
}

type FileReadOp struct {
	FilePath string
}

func (t *FileReadOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.FileRead{
		Output:   op.OutputStreams[0].Var,
		FilePath: t.FilePath,
	}, op.Env, blder)
	return nil
}

type FileWriteOp struct {
	FilePath string
}

func (t *FileWriteOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.FileWrite{
		Input:    op.InputStreams[0].Var,
		FilePath: t.FilePath,
	}, op.Env, blder)
	return nil
}

type FromExecutorOp struct {
	Handle *ExecutorWriteStream
}

func (t *FromExecutorOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	t.Handle.Var = op.OutputStreams[0].Var
	return nil
}

type ToExecutorOp struct {
	Handle *ExecutorReadStream
	Range  Range
}

func (t *ToExecutorOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	t.Handle.Var = op.InputStreams[0].Var
	return nil
}

type StoreOp struct {
	StoreKey string
}

func (t *StoreOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	blder.AtExecutor().AddOp(&ops.Store{
		Var:   op.InputValues[0].Var,
		Key:   t.StoreKey,
		Store: blder.ExecutorPlan.StoreMap,
	})
	return nil
}

type DropOp struct{}

func (t *DropOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.DropStream{
		Input: op.InputStreams[0].Var,
	}, op.Env, blder)
	return nil
}

type SendStreamOp struct{}

func (t *SendStreamOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	toAgt := op.OutputStreams[0].Toes[0].Env.(*AgentEnv)
	addOpByEnv(&ops.SendStream{
		Input: op.InputStreams[0].Var,
		Send:  op.OutputStreams[0].Var,
		Node:  toAgt.Node,
	}, op.Env, blder)
	return nil
}

type GetStreamOp struct{}

func (t *GetStreamOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	fromAgt := op.InputStreams[0].From.Env.(*AgentEnv)
	addOpByEnv(&ops.GetStream{
		Signal: op.OutputValues[0].Var.(*ioswitch.SignalVar),
		Output: op.OutputStreams[0].Var,
		Get:    op.InputStreams[0].Var,
		Node:   fromAgt.Node,
	}, op.Env, blder)
	return nil
}

type SendVarOp struct{}

func (t *SendVarOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	toAgt := op.OutputValues[0].Toes[0].Env.(*AgentEnv)
	addOpByEnv(&ops.SendVar{
		Input: op.InputValues[0].Var,
		Send:  op.OutputValues[0].Var,
		Node:  toAgt.Node,
	}, op.Env, blder)
	return nil
}

type GetVarOp struct{}

func (t *GetVarOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	fromAgt := op.InputValues[0].From.Env.(*AgentEnv)
	addOpByEnv(&ops.GetVar{
		Signal: op.OutputValues[0].Var.(*ioswitch.SignalVar),
		Output: op.OutputValues[1].Var,
		Get:    op.InputValues[0].Var,
		Node:   fromAgt.Node,
	}, op.Env, blder)
	return nil
}

type RangeType struct {
	Range Range
}

func (t *RangeType) GenerateOp(op *Node, blder *PlanBuilder) error {
	addOpByEnv(&ops.Range{
		Input:  op.InputStreams[0].Var,
		Output: op.OutputStreams[0].Var,
		Offset: t.Range.Offset,
		Length: t.Range.Length,
	}, op.Env, blder)
	return nil
}

type HoldUntilOp struct {
}

func (t *HoldUntilOp) GenerateOp(op *Node, blder *PlanBuilder) error {
	o := &ops.HoldUntil{
		Waits: []*ioswitch.SignalVar{op.InputValues[0].Var.(*ioswitch.SignalVar)},
	}

	for i := 0; i < len(op.OutputValues); i++ {
		o.Holds = append(o.Holds, op.InputValues[i+1].Var)
		o.Emits = append(o.Emits, op.OutputValues[i].Var)
	}

	for i := 0; i < len(op.OutputStreams); i++ {
		o.Holds = append(o.Holds, op.InputStreams[i].Var)
		o.Emits = append(o.Emits, op.OutputStreams[i].Var)
	}

	addOpByEnv(o, op.Env, blder)
	return nil
}

func addOpByEnv(op ioswitch.Op, env OpEnv, blder *PlanBuilder) {
	switch env := env.(type) {
	case *AgentEnv:
		blder.AtAgent(env.Node).AddOp(op)
	case *ExecutorEnv:
		blder.AtExecutor().AddOp(op)
	}
}
