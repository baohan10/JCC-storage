package ioswitch

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/types"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

type PlanID string

type VarID int

type Plan struct {
	ID  PlanID `json:"id"`
	Ops []Op   `json:"ops"`
}

type Var interface {
	GetID() VarID
}

var VarUnion = types.NewTypeUnion[Var](
	(*IntVar)(nil),
	(*StringVar)(nil),
)
var _ = serder.UseTypeUnionExternallyTagged(&VarUnion)

type StreamVar struct {
	ID     VarID         `json:"id"`
	Stream io.ReadCloser `json:"-"`
}

func (v *StreamVar) GetID() VarID {
	return v.ID
}

type IntVar struct {
	ID    VarID  `json:"id"`
	Value string `json:"value"`
}

func (v *IntVar) GetID() VarID {
	return v.ID
}

type StringVar struct {
	ID    VarID  `json:"id"`
	Value string `json:"value"`
}

func (v *StringVar) GetID() VarID {
	return v.ID
}

type SignalVar struct {
	ID VarID `json:"id"`
}

func (v *SignalVar) GetID() VarID {
	return v.ID
}

type Op interface {
	Execute(ctx context.Context, sw *Switch) error
}
