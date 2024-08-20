package ioswitch

import (
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
)

func NProps(n *dag.Node) *NodeProps {
	return dag.NProps[*NodeProps](n)
}

func SProps(str *dag.StreamVar) *VarProps {
	return dag.SProps[*VarProps](str)
}

func VProps(v *dag.ValueVar) *VarProps {
	return dag.VProps[*VarProps](v)
}
