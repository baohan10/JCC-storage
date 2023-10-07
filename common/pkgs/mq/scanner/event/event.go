package event

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/common/pkgs/types"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
)

type Event interface {
	Noop()
}

var EventTypeUnino = types.NewTypeUnion[Event]()

type EventBase struct{}

func (e *EventBase) Noop() {}

// 注：此函数必须以var _ = Register[xxx]()的形式被调用，这样才能保证init中RegisterUnionType时
// TypeUnion不是空的。（因为包级变量初始化比init函数调用先进行）
func Register[T Event]() any {
	EventTypeUnino.Add(myreflect.TypeOf[T]())
	return nil
}

func init() {
	mq.RegisterUnionType(EventTypeUnino)
}
