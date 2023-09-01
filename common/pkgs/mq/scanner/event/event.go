package event

import (
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

type Event interface{}

var eventUnionEles = serder.NewTypeNameResolver(true)
var EventTypeUnino = serder.NewTypeUnion[Event]("@type", eventUnionEles)

func Register[T any]() {
	eventUnionEles.Register(myreflect.TypeOf[T]())
}
