package ops

import (
	"context"
	"fmt"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
)

type Store struct {
	Var   exec.Var
	Key   string
	Store *sync.Map
}

func (o *Store) Execute(ctx context.Context, e *exec.Executor) error {
	err := e.BindVars(ctx, o.Var)
	if err != nil {
		return err
	}

	switch v := o.Var.(type) {
	case *exec.IntVar:
		o.Store.Store(o.Key, v.Value)
	case *exec.StringVar:
		o.Store.Store(o.Key, v.Value)
	}

	return nil
}

type StoreType struct {
	StoreKey string
}

func (t *StoreType) InitNode(node *Node) {
	dag.NodeDeclareInputValue(node, 1)
}

func (t *StoreType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	blder.AtExecutor().AddOp(&Store{
		Var:   op.InputValues[0].Props.Var,
		Key:   t.StoreKey,
		Store: blder.DriverPlan.StoreMap,
	})
	return nil
}

func (t *StoreType) String(node *Node) string {
	return fmt.Sprintf("Store[%s]%v%v", t.StoreKey, formatStreamIO(node), formatValueIO(node))
}
