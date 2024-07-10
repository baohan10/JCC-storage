package ioswitch

import (
	"context"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/utils/lo2"
)

type finding struct {
	PlanID   PlanID
	Callback *future.SetValueFuture[*Switch]
}

type Manager struct {
	lock     sync.Mutex
	switchs  map[PlanID]*Switch
	findings []*finding
}

func NewManager() Manager {
	return Manager{
		switchs: make(map[PlanID]*Switch),
	}
}

func (s *Manager) Add(sw *Switch) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.switchs[sw.Plan().ID] = sw
	for i := range s.findings {
		if s.findings[i].PlanID != sw.Plan().ID {
			continue
		}

		s.findings[i].Callback.SetValue(sw)
		s.findings = lo2.RemoveAt(s.findings, i)
	}
}

func (s *Manager) Remove(sw *Switch) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.switchs, sw.Plan().ID)
}

func (s *Manager) FindByID(id PlanID) *Switch {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.switchs[id]
}

func (s *Manager) FindByIDContexted(ctx context.Context, id PlanID) *Switch {
	s.lock.Lock()

	sw := s.switchs[id]
	if sw != nil {
		s.lock.Unlock()
		return sw
	}

	cb := future.NewSetValue[*Switch]()
	f := &finding{
		PlanID:   id,
		Callback: cb,
	}
	s.findings = append(s.findings, f)

	s.lock.Unlock()

	sw, _ = cb.WaitValue(ctx)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.findings = lo2.Remove(s.findings, f)

	return sw
}
