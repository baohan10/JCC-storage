package ops

import (
	"gitlink.org.cn/cloudream/common/pkgs/types"
	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

var OpUnion = serder.UseTypeUnionExternallyTagged(types.Ref(types.NewTypeUnion[ioswitch.Op]()))
