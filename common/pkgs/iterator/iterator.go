package iterator

import (
	"errors"
)

var ErrNoMoreItem = errors.New("no more item")

type Iterator[T any] interface {
	MoveNext() (T, error)
	Close()
}
