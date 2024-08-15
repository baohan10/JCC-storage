package ops

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
)

type FromDriverType struct {
	Handle *exec.DriverWriteStream
}

func (t *FromDriverType) InitNode(node *Node) {
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *FromDriverType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	t.Handle.Var = op.OutputStreams[0].Props.Var.(*exec.StreamVar)
	return nil
}

func (t *FromDriverType) String(node *Node) string {
	return fmt.Sprintf("FromDriver[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type ToDriverType struct {
	Handle *exec.DriverReadStream
	Range  exec.Range
}

func (t *ToDriverType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
}

func (t *ToDriverType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	t.Handle.Var = op.InputStreams[0].Props.Var.(*exec.StreamVar)
	return nil
}

func (t *ToDriverType) String(node *Node) string {
	return fmt.Sprintf("ToDriver[%v+%v]%v%v", t.Range.Offset, t.Range.Length, formatStreamIO(node), formatValueIO(node))
}
