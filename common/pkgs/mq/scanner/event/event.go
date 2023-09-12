package event

import (
	"gitlink.org.cn/cloudream/common/pkgs/types"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
)

type Event interface {
	Noop()
}

var EventTypeUnino = types.NewTypeUnion[Event]()

type EventBase struct{}

func (e *EventBase) Noop() {}

func Register[T any]() {
	EventTypeUnino.Add(myreflect.TypeOf[T]())
}
