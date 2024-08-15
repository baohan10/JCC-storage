package ops

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
)

func init() {
	OpUnion.AddT((*DropStream)(nil))
}

type DropStream struct {
	Input *exec.StreamVar `json:"input"`
}

func (o *DropStream) Execute(ctx context.Context, e *exec.Executor) error {
	err := e.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}

	for {
		buf := make([]byte, 1024*8)
		_, err = o.Input.Stream.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

type DropType struct{}

func (t *DropType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
}

func (t *DropType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&DropStream{
		Input: op.InputStreams[0].Props.Var.(*exec.StreamVar),
	}, op.Env, blder)
	return nil
}

func (t *DropType) String(node *Node) string {
	return fmt.Sprintf("Drop[]%v%v", formatStreamIO(node), formatValueIO(node))
}
