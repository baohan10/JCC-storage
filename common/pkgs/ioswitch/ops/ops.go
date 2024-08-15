package ops

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/types"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

var OpUnion = serder.UseTypeUnionExternallyTagged(types.Ref(types.NewTypeUnion[exec.Op]()))

type AgentWorker struct {
	Node cdssdk.Node
}

func (w *AgentWorker) GetAddress() string {
	// TODO 选择地址
	return fmt.Sprintf("%v:%v", w.Node.ExternalIP, w.Node.ExternalGRPCPort)
}

func (w *AgentWorker) Equals(worker dag.WorkerInfo) bool {
	aw, ok := worker.(*AgentWorker)
	if !ok {
		return false
	}

	return w.Node.NodeID == aw.Node.NodeID
}

type NodeProps struct {
	From From
	To   To
}

type ValueVarType int

const (
	StringValueVar ValueVarType = iota
	SignalValueVar
)

type VarProps struct {
	StreamIndex int          // 流的编号，只在StreamVar上有意义
	ValueType   ValueVarType // 值类型，只在ValueVar上有意义
	Var         exec.Var     // 生成Plan的时候创建的对应的Var
}

type Graph = dag.Graph[NodeProps, VarProps]

type Node = dag.Node[NodeProps, VarProps]

type StreamVar = dag.StreamVar[NodeProps, VarProps]

type ValueVar = dag.ValueVar[NodeProps, VarProps]

func addOpByEnv(op exec.Op, env dag.NodeEnv, blder *exec.PlanBuilder) {
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
