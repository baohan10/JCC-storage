package event

import (
	"gitlink.org.cn/cloudream/common/pkgs/types"
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
)

type Event interface{}

var EventTypeUnino = types.NewTypeUnion[Event]()

func Register[T any]() {
	EventTypeUnino.Add(myreflect.TypeOf[T]())
}
