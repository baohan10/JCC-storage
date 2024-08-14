package plans

import (
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"
)

type IPFSReadType struct {
	FileHash string
	Option   ipfs.ReadOption
}

func (t *IPFSReadType) InitNode(node *Node) {
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *IPFSReadType) GenerateOp(node *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.IPFSRead{
		Output:   node.OutputStreams[0].Props.Var.(*ioswitch.StreamVar),
		FileHash: t.FileHash,
		Option:   t.Option,
	}, node.Env, blder)
	return nil
}

func (t *IPFSReadType) String(node *Node) string {
	return fmt.Sprintf("IPFSRead[%s,%v+%v]%v%v", t.FileHash, t.Option.Offset, t.Option.Length, formatStreamIO(node), formatValueIO(node))
}

type IPFSWriteType struct {
	FileHashStoreKey string
	Range            Range
}

func (t *IPFSWriteType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
	dag.NodeNewOutputValue(node, VarProps{})
}

func (t *IPFSWriteType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.IPFSWrite{
		Input:    op.InputStreams[0].Props.Var.(*ioswitch.StreamVar),
		FileHash: op.OutputValues[0].Props.Var.(*ioswitch.StringVar),
	}, op.Env, blder)
	return nil
}

func (t *IPFSWriteType) String(node *Node) string {
	return fmt.Sprintf("IPFSWrite[%s,%v+%v](%v>)", t.FileHashStoreKey, t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
}

type ChunkedSplitType struct {
	OutputCount int
	ChunkSize   int
}

func (t *ChunkedSplitType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
	for i := 0; i < t.OutputCount; i++ {
		dag.NodeNewOutputStream(node, VarProps{})
	}
}

func (t *ChunkedSplitType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.ChunkedSplit{
		Input: op.InputStreams[0].Props.Var.(*ioswitch.StreamVar),
		Outputs: lo.Map(op.OutputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar {
			return v.Props.Var.(*ioswitch.StreamVar)
		}),
		ChunkSize:    t.ChunkSize,
		PaddingZeros: true,
	}, op.Env, blder)
	return nil
}

func (t *ChunkedSplitType) String(node *Node) string {
	return fmt.Sprintf("ChunkedSplit[%v]", t.ChunkSize, formatStreamIO(node), formatValueIO(node))
}

type ChunkedJoinType struct {
	InputCount int
	ChunkSize  int
}

func (t *ChunkedJoinType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, t.InputCount)
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *ChunkedJoinType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.ChunkedJoin{
		Inputs: lo.Map(op.InputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar {
			return v.Props.Var.(*ioswitch.StreamVar)
		}),
		Output:    op.OutputStreams[0].Props.Var.(*ioswitch.StreamVar),
		ChunkSize: t.ChunkSize,
	}, op.Env, blder)
	return nil
}

func (t *ChunkedJoinType) String(node *Node) string {
	return fmt.Sprintf("ChunkedJoin[%v]", t.ChunkSize, formatStreamIO(node), formatValueIO(node))
}

type CloneStreamType struct{}

func (t *CloneStreamType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
}

func (t *CloneStreamType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.CloneStream{
		Input: op.InputStreams[0].Props.Var.(*ioswitch.StreamVar),
		Outputs: lo.Map(op.OutputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar {
			return v.Props.Var.(*ioswitch.StreamVar)
		}),
	}, op.Env, blder)
	return nil
}

func (t *CloneStreamType) NewOutput(node *Node) *StreamVar {
	return dag.NodeNewOutputStream(node, VarProps{})
}

func (t *CloneStreamType) String(node *Node) string {
	return fmt.Sprintf("CloneStream[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type CloneVarType struct{}

func (t *CloneVarType) InitNode(node *Node) {
	dag.NodeDeclareInputValue(node, 1)
}

func (t *CloneVarType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.CloneVar{
		Raw: op.InputValues[0].Props.Var,
		Cloneds: lo.Map(op.OutputValues, func(v *ValueVar, idx int) ioswitch.Var {
			return v.Props.Var
		}),
	}, op.Env, blder)
	return nil
}

func (t *CloneVarType) NewOutput(node *Node) *ValueVar {
	return dag.NodeNewOutputValue(node, VarProps{})
}

func (t *CloneVarType) String(node *Node) string {
	return fmt.Sprintf("CloneVar[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type MultiplyType struct {
	EC cdssdk.ECRedundancy
}

func (t *MultiplyType) InitNode(node *Node) {}

func (t *MultiplyType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	var inputIdxs []int
	var outputIdxs []int
	for _, in := range op.InputStreams {
		inputIdxs = append(inputIdxs, in.Props.StreamIndex)
	}
	for _, out := range op.OutputStreams {
		outputIdxs = append(outputIdxs, out.Props.StreamIndex)
	}

	rs, err := ec.NewRs(t.EC.K, t.EC.N)
	coef, err := rs.GenerateMatrix(inputIdxs, outputIdxs)
	if err != nil {
		return err
	}

	addOpByEnv(&ops.ECMultiply{
		Coef:      coef,
		Inputs:    lo.Map(op.InputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar { return v.Props.Var.(*ioswitch.StreamVar) }),
		Outputs:   lo.Map(op.OutputStreams, func(v *StreamVar, idx int) *ioswitch.StreamVar { return v.Props.Var.(*ioswitch.StreamVar) }),
		ChunkSize: t.EC.ChunkSize,
	}, op.Env, blder)
	return nil
}

func (t *MultiplyType) AddInput(node *Node, str *StreamVar) {
	node.InputStreams = append(node.InputStreams, str)
	str.To(node, len(node.InputStreams)-1)
}

func (t *MultiplyType) NewOutput(node *Node, dataIndex int) *StreamVar {
	return dag.NodeNewOutputStream(node, VarProps{StreamIndex: dataIndex})
}

func (t *MultiplyType) String(node *Node) string {
	return fmt.Sprintf("Multiply[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type FileReadType struct {
	FilePath string
}

func (t *FileReadType) InitNode(node *Node) {
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *FileReadType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.FileRead{
		Output:   op.OutputStreams[0].Props.Var.(*ioswitch.StreamVar),
		FilePath: t.FilePath,
	}, op.Env, blder)
	return nil
}

func (t *FileReadType) String(node *Node) string {
	return fmt.Sprintf("FileRead[%s]%v%v", t.FilePath, formatStreamIO(node), formatValueIO(node))
}

type FileWriteType struct {
	FilePath string
}

func (t *FileWriteType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
}

func (t *FileWriteType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.FileWrite{
		Input:    op.InputStreams[0].Props.Var.(*ioswitch.StreamVar),
		FilePath: t.FilePath,
	}, op.Env, blder)
	return nil
}

type FromExecutorType struct {
	Handle *exec.ExecutorWriteStream
}

func (t *FromExecutorType) InitNode(node *Node) {
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *FromExecutorType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	t.Handle.Var = op.OutputStreams[0].Props.Var.(*ioswitch.StreamVar)
	return nil
}

func (t *FromExecutorType) String(node *Node) string {
	return fmt.Sprintf("FromExecutor[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type ToExecutorType struct {
	Handle *exec.ExecutorReadStream
	Range  Range
}

func (t *ToExecutorType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
}

func (t *ToExecutorType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	t.Handle.Var = op.InputStreams[0].Props.Var.(*ioswitch.StreamVar)
	return nil
}

func (t *ToExecutorType) String(node *Node) string {
	return fmt.Sprintf("ToExecutor[%v+%v]%v%v", t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
}

type StoreType struct {
	StoreKey string
}

func (t *StoreType) InitNode(node *Node) {
	dag.NodeDeclareInputValue(node, 1)
}

func (t *StoreType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	blder.AtExecutor().AddOp(&ops.Store{
		Var:   op.InputValues[0].Props.Var,
		Key:   t.StoreKey,
		Store: blder.ExecutorPlan.StoreMap,
	})
	return nil
}

func (t *StoreType) String(node *Node) string {
	return fmt.Sprintf("Store[%s]%v%v", t.StoreKey, formatStreamIO(node), formatValueIO(node))
}

type DropType struct{}

func (t *DropType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
}

func (t *DropType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.DropStream{
		Input: op.InputStreams[0].Props.Var.(*ioswitch.StreamVar),
	}, op.Env, blder)
	return nil
}

func (t *DropType) String(node *Node) string {
	return fmt.Sprintf("Drop[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type SendStreamType struct {
}

func (t *SendStreamType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *SendStreamType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	toAgt := op.OutputStreams[0].Toes[0].Node.Env.Worker.(*AgentWorker)
	addOpByEnv(&ops.SendStream{
		Input: op.InputStreams[0].Props.Var.(*ioswitch.StreamVar),
		Send:  op.OutputStreams[0].Props.Var.(*ioswitch.StreamVar),
		Node:  toAgt.Node,
	}, op.Env, blder)
	return nil
}

func (t *SendStreamType) String(node *Node) string {
	return fmt.Sprintf("SendStream[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type SendVarType struct {
}

func (t *SendVarType) InitNode(node *Node) {
	dag.NodeDeclareInputValue(node, 1)
	dag.NodeNewOutputValue(node, VarProps{})
}

func (t *SendVarType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	toAgt := op.OutputValues[0].Toes[0].Node.Env.Worker.(*AgentWorker)
	addOpByEnv(&ops.SendVar{
		Input: op.InputValues[0].Props.Var,
		Send:  op.OutputValues[0].Props.Var,
		Node:  toAgt.Node,
	}, op.Env, blder)
	return nil
}

func (t *SendVarType) String(node *Node) string {
	return fmt.Sprintf("SendVar[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type GetStreamType struct {
}

func (t *GetStreamType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
	dag.NodeNewOutputValue(node, VarProps{})
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *GetStreamType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	fromAgt := op.InputStreams[0].From.Node.Env.Worker.(*AgentWorker)
	addOpByEnv(&ops.GetStream{
		Signal: op.OutputValues[0].Props.Var.(*ioswitch.SignalVar),
		Output: op.OutputStreams[0].Props.Var.(*ioswitch.StreamVar),
		Target: op.InputStreams[0].Props.Var.(*ioswitch.StreamVar),
		Node:   fromAgt.Node,
	}, op.Env, blder)
	return nil
}

func (t *GetStreamType) String(node *Node) string {
	return fmt.Sprintf("GetStream[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type GetVaType struct {
}

func (t *GetVaType) InitNode(node *Node) {
	dag.NodeDeclareInputValue(node, 1)
	dag.NodeNewOutputValue(node, VarProps{})
	dag.NodeNewOutputValue(node, VarProps{})
}

func (t *GetVaType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	fromAgt := op.InputValues[0].From.Node.Env.Worker.(*AgentWorker)
	addOpByEnv(&ops.GetVar{
		Signal: op.OutputValues[0].Props.Var.(*ioswitch.SignalVar),
		Output: op.OutputValues[1].Props.Var,
		Target: op.InputValues[0].Props.Var,
		Node:   fromAgt.Node,
	}, op.Env, blder)
	return nil
}

func (t *GetVaType) String(node *Node) string {
	return fmt.Sprintf("GetVar[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type RangeType struct {
	Range Range
}

func (t *RangeType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *RangeType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ops.Range{
		Input:  op.InputStreams[0].Props.Var.(*ioswitch.StreamVar),
		Output: op.OutputStreams[0].Props.Var.(*ioswitch.StreamVar),
		Offset: t.Range.Offset,
		Length: t.Range.Length,
	}, op.Env, blder)
	return nil
}

func (t *RangeType) String(node *Node) string {
	return fmt.Sprintf("Range[%v+%v]%v%v", t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
}

type HoldUntilType struct {
}

func (t *HoldUntilType) InitNode(node *Node) {
	dag.NodeDeclareInputValue(node, 1)
}

func (t *HoldUntilType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	o := &ops.HoldUntil{
		Waits: []*ioswitch.SignalVar{op.InputValues[0].Props.Var.(*ioswitch.SignalVar)},
	}

	for i := 0; i < len(op.OutputValues); i++ {
		o.Holds = append(o.Holds, op.InputValues[i+1].Props.Var)
		o.Emits = append(o.Emits, op.OutputValues[i].Props.Var)
	}

	for i := 0; i < len(op.OutputStreams); i++ {
		o.Holds = append(o.Holds, op.InputStreams[i].Props.Var)
		o.Emits = append(o.Emits, op.OutputStreams[i].Props.Var)
	}

	addOpByEnv(o, op.Env, blder)
	return nil
}

func (t *HoldUntilType) String(node *Node) string {
	return fmt.Sprintf("HoldUntil[]%v%v", formatStreamIO(node), formatValueIO(node))
}

func addOpByEnv(op ioswitch.Op, env dag.NodeEnv, blder *exec.PlanBuilder) {
	switch env.Type {
	case dag.EnvWorker:
		blder.AtAgent(env.Worker.(*AgentWorker).Node).AddOp(op)
	case dag.EnvExecutor:
		blder.AtExecutor().AddOp(op)
	}
}

func formatStreamIO(node *Node) string {
	is := ""
	for i, in := range node.InputStreams {
		if i > 0 {
			is += ","
		}

		if in == nil {
			is += "."
		} else {
			is += fmt.Sprintf("%v", in.ID)
		}
	}

	os := ""
	for i, out := range node.OutputStreams {
		if i > 0 {
			os += ","
		}

		if out == nil {
			os += "."
		} else {
			os += fmt.Sprintf("%v", out.ID)
		}
	}

	if is == "" && os == "" {
		return ""
	}

	return fmt.Sprintf("S{%s>%s}", is, os)
}

func formatValueIO(node *Node) string {
	is := ""
	for i, in := range node.InputValues {
		if i > 0 {
			is += ","
		}

		if in == nil {
			is += "."
		} else {
			is += fmt.Sprintf("%v", in.ID)
		}
	}

	os := ""
	for i, out := range node.OutputValues {
		if i > 0 {
			os += ","
		}

		if out == nil {
			os += "."
		} else {
			os += fmt.Sprintf("%v", out.ID)
		}
	}

	if is == "" && os == "" {
		return ""
	}

	return fmt.Sprintf("V{%s>%s}", is, os)
}
