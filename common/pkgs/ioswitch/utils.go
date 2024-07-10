package ioswitch

import (
	"fmt"
	"reflect"
)

func AssignVar(from Var, to Var) error {
	if reflect.TypeOf(from) != reflect.TypeOf(to) {
		return fmt.Errorf("cannot assign %T to %T", from, to)
	}

	switch from := from.(type) {
	case *IntVar:
		to.(*IntVar).Value = from.Value
	case *StringVar:
		to.(*StringVar).Value = from.Value
	}

	return nil
}
