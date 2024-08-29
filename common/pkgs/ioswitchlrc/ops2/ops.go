package ops2

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
)

func formatStreamIO(node *dag.Node) string {
	is := ""
	for i, in := range node.InputStreams {
		if i > 0 {
			is += ","
		}

		if in == nil {
			is += "."
		} else {
			is += fmt.Sprintf("%v", in.ID)
		}
	}

	os := ""
	for i, out := range node.OutputStreams {
		if i > 0 {
			os += ","
		}

		if out == nil {
			os += "."
		} else {
			os += fmt.Sprintf("%v", out.ID)
		}
	}

	if is == "" && os == "" {
		return ""
	}

	return fmt.Sprintf("S{%s>%s}", is, os)
}

func formatValueIO(node *dag.Node) string {
	is := ""
	for i, in := range node.InputValues {
		if i > 0 {
			is += ","
		}

		if in == nil {
			is += "."
		} else {
			is += fmt.Sprintf("%v", in.ID)
		}
	}

	os := ""
	for i, out := range node.OutputValues {
		if i > 0 {
			os += ","
		}

		if out == nil {
			os += "."
		} else {
			os += fmt.Sprintf("%v", out.ID)
		}
	}

	if is == "" && os == "" {
		return ""
	}

	return fmt.Sprintf("V{%s>%s}", is, os)
}
