package ioswitch

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/lo"
)

var ErrPlanFinished = errors.New("plan is finished")

var ErrPlanNotFound = errors.New("plan not found")

type OpState string

const (
	OpPending  OpState = "Pending"
	OpFinished OpState = "Finished"
)

type Oping struct {
	State OpState
}

type PlanResult struct {
	Values map[string]any `json:"values"`
}

type Planning struct {
	plan         Plan
	opings       []Oping
	resultValues map[string]any
	callback     *future.SetValueFuture[PlanResult]

	readys    map[StreamID]Stream
	waittings []*Watting
}

func NewPlanning(plan Plan) Planning {
	planning := Planning{
		plan:     plan,
		callback: future.NewSetValue[PlanResult](),
		readys:   make(map[StreamID]Stream),
	}

	for _ = range plan.Ops {
		oping := Oping{
			State: OpPending,
		}
		planning.opings = append(planning.opings, oping)
	}

	return planning
}

func (p *Planning) IsCompleted() bool {
	for _, oping := range p.opings {
		if oping.State != OpFinished {
			return false
		}
	}

	return true
}

func (p *Planning) MakeResult() PlanResult {
	return PlanResult{
		Values: p.resultValues,
	}
}

type Watting struct {
	WaitIDs  []StreamID
	Readys   []Stream
	Callback *future.SetValueFuture[[]Stream]
}

func (w *Watting) TryReady(str Stream) bool {
	for i, id := range w.WaitIDs {
		if id == str.ID {
			w.Readys[i] = str
			return true
		}
	}

	return false
}

func (c *Watting) IsAllReady() bool {
	for _, s := range c.Readys {
		if s.Stream == nil {
			return false
		}
	}

	return true
}

func (w *Watting) Complete() {
	w.Callback.SetValue(w.Readys)
}

func (w *Watting) Cancel(err error) {
	w.Callback.SetError(err)
}

type Switch struct {
	lock      sync.Mutex
	plannings map[PlanID]*Planning
}

func NewSwitch() Switch {
	return Switch{
		plannings: make(map[PlanID]*Planning),
	}
}

func (s *Switch) SetupPlan(plan Plan) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.plannings[plan.ID]; ok {
		return fmt.Errorf("plan id exists")
	}

	planning := NewPlanning(plan)
	s.plannings[plan.ID] = &planning
	return nil
}

func (s *Switch) ExecutePlan(id PlanID) (PlanResult, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	planning, ok := s.plannings[id]
	if !ok {
		return PlanResult{}, fmt.Errorf("plan not found")
	}

	for i, op := range planning.plan.Ops {
		idx := i
		o := op
		go func() {
			err := o.Execute(s, id)

			s.lock.Lock()
			defer s.lock.Unlock()

			if err != nil {
				logger.Std.Warnf("exeucting op: %s", err.Error())
				s.cancelPlan(id)
				return
			}

			planning.opings[idx].State = OpFinished
			if planning.IsCompleted() {
				s.completePlan(id)
			}
		}()
	}

	return planning.callback.WaitValue(context.TODO())
}

func (s *Switch) CancelPlan(id PlanID) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.cancelPlan(id)
}

func (s *Switch) cancelPlan(id PlanID) {
	plan, ok := s.plannings[id]
	if !ok {
		return
	}

	delete(s.plannings, id)

	for _, s := range plan.readys {
		s.Stream.Close()
	}

	for _, c := range plan.waittings {
		c.Callback.SetError(ErrPlanFinished)
	}

	plan.callback.SetError(fmt.Errorf("plan cancelled"))
}

func (s *Switch) completePlan(id PlanID) {
	plan, ok := s.plannings[id]
	if !ok {
		return
	}

	delete(s.plannings, id)

	for _, s := range plan.readys {
		s.Stream.Close()
	}

	for _, c := range plan.waittings {
		c.Callback.SetError(ErrPlanFinished)
	}

	plan.callback.SetValue(plan.MakeResult())
}

func (s *Switch) StreamReady(planID PlanID, stream Stream) {
	s.lock.Lock()
	defer s.lock.Unlock()

	plan, ok := s.plannings[planID]
	if !ok {
		//TODO 处理错误
		return
	}

	for i, wa := range plan.waittings {
		if !wa.TryReady(stream) {
			continue
		}

		if !wa.IsAllReady() {
			return
		}

		plan.waittings = lo.RemoveAt(plan.waittings, i)
		wa.Complete()
		return
	}

	plan.readys[stream.ID] = stream
}

func (s *Switch) WaitStreams(planID PlanID, streamIDs ...StreamID) ([]Stream, error) {

	s.lock.Lock()
	defer s.lock.Unlock()

	plan, ok := s.plannings[planID]
	if !ok {
		return nil, ErrPlanNotFound
	}

	allReady := true
	readys := make([]Stream, len(streamIDs))
	for i, id := range streamIDs {
		str, ok := plan.readys[id]
		if !ok {
			allReady = false
			continue
		}

		readys[i] = str
		delete(plan.readys, id)
	}

	if allReady {
		return readys, nil
	}

	callback := future.NewSetValue[[]Stream]()

	plan.waittings = append(plan.waittings, &Watting{
		WaitIDs:  streamIDs,
		Readys:   readys,
		Callback: callback,
	})

	return callback.WaitValue(context.TODO())
}

func (s *Switch) AddResultValue(planID PlanID, rets ...ResultKV) {
	s.lock.Lock()
	defer s.lock.Unlock()

	plan, ok := s.plannings[planID]
	if !ok {
		return
	}

	for _, ret := range rets {
		plan.resultValues[ret.Key] = ret.Value
	}
}
