package ops2

import (
	"context"
	"fmt"
	"io"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/utils"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"golang.org/x/sync/semaphore"
)

func init() {
	exec.UseOp[*CloneStream]()
	exec.UseOp[*CloneVar]()
}

type CloneStream struct {
	Raw     *exec.StreamVar   `json:"raw"`
	Cloneds []*exec.StreamVar `json:"cloneds"`
}

func (o *CloneStream) Execute(ctx context.Context, e *exec.Executor) error {
	err := e.BindVars(ctx, o.Raw)
	if err != nil {
		return err
	}
	defer o.Raw.Stream.Close()

	cloned := io2.Clone(o.Raw.Stream, len(o.Cloneds))

	sem := semaphore.NewWeighted(int64(len(o.Cloneds)))
	for i, s := range cloned {
		sem.Acquire(ctx, 1)

		o.Cloneds[i].Stream = io2.AfterReadClosedOnce(s, func(closer io.ReadCloser) {
			sem.Release(1)
		})
	}
	exec.PutArrayVars(e, o.Cloneds)

	return sem.Acquire(ctx, int64(len(o.Cloneds)))
}

func (o *CloneStream) String() string {
	return fmt.Sprintf("CloneStream %v -> (%v)", o.Raw.ID, utils.FormatVarIDs(o.Cloneds))
}

type CloneVar struct {
	Raw     exec.Var   `json:"raw"`
	Cloneds []exec.Var `json:"cloneds"`
}

func (o *CloneVar) Execute(ctx context.Context, e *exec.Executor) error {
	err := e.BindVars(ctx, o.Raw)
	if err != nil {
		return err
	}

	for _, v := range o.Cloneds {
		if err := exec.AssignVar(o.Raw, v); err != nil {
			return fmt.Errorf("clone var: %w", err)
		}
	}
	e.PutVars(o.Cloneds...)

	return nil
}

func (o *CloneVar) String() string {
	return fmt.Sprintf("CloneStream %v -> (%v)", o.Raw.GetID(), utils.FormatVarIDs(o.Cloneds))
}

type CloneStreamType struct{}

func (t *CloneStreamType) InitNode(node *dag.Node) {
	dag.NodeDeclareInputStream(node, 1)
}

func (t *CloneStreamType) GenerateOp(op *dag.Node) (exec.Op, error) {
	return &CloneStream{
		Raw: op.InputStreams[0].Var,
		Cloneds: lo.Map(op.OutputStreams, func(v *dag.StreamVar, idx int) *exec.StreamVar {
			return v.Var
		}),
	}, nil
}

func (t *CloneStreamType) NewOutput(node *dag.Node) *dag.StreamVar {
	return dag.NodeNewOutputStream(node, nil)
}

func (t *CloneStreamType) String(node *dag.Node) string {
	return fmt.Sprintf("CloneStream[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type CloneVarType struct{}

func (t *CloneVarType) InitNode(node *dag.Node) {
	dag.NodeDeclareInputValue(node, 1)
}

func (t *CloneVarType) GenerateOp(op *dag.Node) (exec.Op, error) {
	return &CloneVar{
		Raw: op.InputValues[0].Var,
		Cloneds: lo.Map(op.OutputValues, func(v *dag.ValueVar, idx int) exec.Var {
			return v.Var
		}),
	}, nil
}

func (t *CloneVarType) NewOutput(node *dag.Node) *dag.ValueVar {
	return dag.NodeNewOutputValue(node, node.InputValues[0].Type, nil)
}

func (t *CloneVarType) String(node *dag.Node) string {
	return fmt.Sprintf("CloneVar[]%v%v", formatStreamIO(node), formatValueIO(node))
}
