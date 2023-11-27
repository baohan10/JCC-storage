package ioswitch

import (
	"io"
)

type PlanID string

type StreamID string

type Plan struct {
	ID  PlanID
	Ops []Op
}

type Stream struct {
	ID     StreamID
	Stream io.ReadCloser
}

func NewStream(id StreamID, stream io.ReadCloser) Stream {
	return Stream{
		ID:     id,
		Stream: stream,
	}
}

type Op interface {
	Execute(sw *Switch, planID PlanID) error
}

type ResultKV struct {
	Key   string
	Value any
}
