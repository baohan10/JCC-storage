package ioswitchlrc

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
)

type NodeProps struct {
	From From
	To   To
}

type ValueVarType int

const (
	StringValueVar ValueVarType = iota
	SignalValueVar
)

type VarProps struct {
	StreamIndex int          // 流的编号，只在StreamVar上有意义
	ValueType   ValueVarType // 值类型，只在ValueVar上有意义
	Var         exec.Var     // 生成Plan的时候创建的对应的Var
}
