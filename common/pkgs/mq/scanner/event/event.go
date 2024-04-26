package event

import (
	"gitlink.org.cn/cloudream/common/pkgs/types"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

type Event interface {
	Noop()
}

var EventTypeUnino = serder.UseTypeUnionExternallyTagged(types.Ref(types.NewTypeUnion[Event]()))

type EventBase struct{}

func (e *EventBase) Noop() {}

// 只能在init函数中调用，因为包级变量初始化比init函数调用先进行
func Register[T Event]() any {
	EventTypeUnino.Add(reflect2.TypeOf[T]())
	return nil
}
