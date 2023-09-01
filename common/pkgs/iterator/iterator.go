package iterator

import (
	"gitlink.org.cn/cloudream/common/pkgs/iterator"
)

var ErrNoMoreItem = iterator.ErrNoMoreItem

type Iterator[T any] iterator.Iterator[T]
