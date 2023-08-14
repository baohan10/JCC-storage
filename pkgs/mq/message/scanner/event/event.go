package event

import (
	myreflect "gitlink.org.cn/cloudream/common/utils/reflect"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

var typeResolver = serder.NewTypeNameResolver(true)

var serderOption = serder.TypedSerderOption{
	TypeResolver:  &typeResolver,
	TypeFieldName: "@type",
}

func MapToMessage(m map[string]any) (any, error) {
	return serder.TypedMapToObject(m, serderOption)
}

func MessageToMap(msg any) (map[string]any, error) {
	return serder.ObjectToTypedMap(msg, serderOption)
}

func Register[T any]() {
	typeResolver.Register(myreflect.TypeOf[T]())
}
