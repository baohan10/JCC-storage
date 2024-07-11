package ioswitch

import (
	"context"
	"fmt"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/common/utils/sync2"
)

type bindingVars struct {
	Waittings []Var
	Bindeds   []Var
	Callback  *future.SetVoidFuture
}

type Switch struct {
	plan     Plan
	vars     map[VarID]Var
	bindings []*bindingVars
	lock     sync.Mutex
}

func NewSwitch(plan Plan) *Switch {
	planning := Switch{
		plan: plan,
		vars: make(map[VarID]Var),
	}

	return &planning
}

func (s *Switch) Plan() *Plan {
	return &s.plan
}

func (s *Switch) Run(ctx context.Context) error {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	return sync2.ParallelDo(s.plan.Ops, func(o Op, idx int) error {
		err := o.Execute(ctx2, s)

		s.lock.Lock()
		defer s.lock.Unlock()

		if err != nil {
			cancel()
			return err
		}

		return nil
	})
}

func (s *Switch) BindVars(ctx context.Context, vs ...Var) error {
	s.lock.Lock()

	callback := future.NewSetVoid()
	binding := &bindingVars{
		Callback: callback,
	}

	for _, v := range vs {
		v2 := s.vars[v.GetID()]
		if v2 == nil {
			binding.Waittings = append(binding.Waittings, v)
			continue
		}

		if err := AssignVar(v2, v); err != nil {
			s.lock.Unlock()
			return fmt.Errorf("assign var %v to %v: %w", v2.GetID(), v.GetID(), err)
		}

		binding.Bindeds = append(binding.Bindeds, v)
	}

	if len(binding.Waittings) == 0 {
		s.lock.Unlock()
		return nil
	}

	s.bindings = append(s.bindings, binding)
	s.lock.Unlock()

	err := callback.Wait(ctx)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.bindings = lo2.Remove(s.bindings, binding)

	return err
}

func (s *Switch) PutVars(vs ...Var) {
	s.lock.Lock()
	defer s.lock.Unlock()

loop:
	for _, v := range vs {
		for ib, b := range s.bindings {
			for iw, w := range b.Waittings {
				if w.GetID() != v.GetID() {
					continue
				}

				if err := AssignVar(v, w); err != nil {
					b.Callback.SetError(fmt.Errorf("assign var %v to %v: %w", v.GetID(), w.GetID(), err))
					// 绑定类型不对，说明生成的执行计划有问题，怎么处理都可以，因为最终会执行失败
					continue loop
				}

				b.Bindeds = append(b.Bindeds, w)
				b.Waittings = lo2.RemoveAt(b.Waittings, iw)
				if len(b.Waittings) == 0 {
					b.Callback.SetVoid()
					s.bindings = lo2.RemoveAt(s.bindings, ib)
				}

				// 绑定成功，继续最外层循环
				continue loop
			}

		}

		// 如果没有绑定，则直接放入变量表中
		s.vars[v.GetID()] = v
	}
}

func BindArrayVars[T Var](sw *Switch, ctx context.Context, vs []T) error {
	var vs2 []Var
	for _, v := range vs {
		vs2 = append(vs2, v)
	}

	return sw.BindVars(ctx, vs2...)
}

func PutArrayVars[T Var](sw *Switch, vs []T) {
	var vs2 []Var
	for _, v := range vs {
		vs2 = append(vs2, v)
	}

	sw.PutVars(vs2...)
}
