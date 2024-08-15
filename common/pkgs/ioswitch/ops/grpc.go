package ops

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
)

func init() {
	OpUnion.AddT((*SendStream)(nil))
	OpUnion.AddT((*GetStream)(nil))
	OpUnion.AddT((*SendVar)(nil))
	OpUnion.AddT((*GetVar)(nil))
}

type SendStream struct {
	Input *exec.StreamVar `json:"input"`
	Send  *exec.StreamVar `json:"send"`
	Node  cdssdk.Node     `json:"node"`
}

func (o *SendStream) Execute(ctx context.Context, e *exec.Executor) error {
	err := e.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("sending stream %v as %v to node %v", o.Input.ID, o.Send.ID, o.Node)

	// 发送后流的ID不同
	err = agtCli.SendStream(ctx, e.Plan().ID, o.Send.ID, o.Input.Stream)
	if err != nil {
		return fmt.Errorf("sending stream: %w", err)
	}

	return nil
}

type GetStream struct {
	Signal *exec.SignalVar `json:"signal"`
	Target *exec.StreamVar `json:"target"`
	Output *exec.StreamVar `json:"output"`
	Node   cdssdk.Node     `json:"node"`
}

func (o *GetStream) Execute(ctx context.Context, e *exec.Executor) error {
	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("getting stream %v as %v from node %v", o.Target.ID, o.Output.ID, o.Node)

	str, err := agtCli.GetStream(e.Plan().ID, o.Target.ID, o.Signal)
	if err != nil {
		return fmt.Errorf("getting stream: %w", err)
	}

	fut := future.NewSetVoid()
	// 获取后送到本地的流ID是不同的
	o.Output.Stream = io2.AfterReadClosedOnce(str, func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	e.PutVars(o.Output)

	return fut.Wait(ctx)
}

type SendVar struct {
	Input exec.Var    `json:"input"`
	Send  exec.Var    `json:"send"`
	Node  cdssdk.Node `json:"node"`
}

func (o *SendVar) Execute(ctx context.Context, e *exec.Executor) error {
	err := e.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}

	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("sending var %v as %v to node %v", o.Input.GetID(), o.Send.GetID(), o.Node)

	exec.AssignVar(o.Input, o.Send)
	err = agtCli.SendVar(ctx, e.Plan().ID, o.Send)
	if err != nil {
		return fmt.Errorf("sending var: %w", err)
	}

	return nil
}

type GetVar struct {
	Signal *exec.SignalVar `json:"signal"`
	Target exec.Var        `json:"target"`
	Output exec.Var        `json:"output"`
	Node   cdssdk.Node     `json:"node"`
}

func (o *GetVar) Execute(ctx context.Context, e *exec.Executor) error {
	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("getting var %v as %v from node %v", o.Target.GetID(), o.Output.GetID(), o.Node)

	v2, err := agtCli.GetVar(ctx, e.Plan().ID, o.Target, o.Signal)
	if err != nil {
		return fmt.Errorf("getting var: %w", err)
	}
	exec.AssignVar(v2, o.Output)
	e.PutVars(o.Output)

	return nil
}

type SendStreamType struct {
}

func (t *SendStreamType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *SendStreamType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	toAgt := op.OutputStreams[0].Toes[0].Node.Env.Worker.(*AgentWorker)
	addOpByEnv(&SendStream{
		Input: op.InputStreams[0].Props.Var.(*exec.StreamVar),
		Send:  op.OutputStreams[0].Props.Var.(*exec.StreamVar),
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
	addOpByEnv(&SendVar{
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
	addOpByEnv(&GetStream{
		Signal: op.OutputValues[0].Props.Var.(*exec.SignalVar),
		Output: op.OutputStreams[0].Props.Var.(*exec.StreamVar),
		Target: op.InputStreams[0].Props.Var.(*exec.StreamVar),
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
	addOpByEnv(&GetVar{
		Signal: op.OutputValues[0].Props.Var.(*exec.SignalVar),
		Output: op.OutputValues[1].Props.Var,
		Target: op.InputValues[0].Props.Var,
		Node:   fromAgt.Node,
	}, op.Env, blder)
	return nil
}

func (t *GetVaType) String(node *Node) string {
	return fmt.Sprintf("GetVar[]%v%v", formatStreamIO(node), formatValueIO(node))
}
