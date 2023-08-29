package lockprovider

import (
	"fmt"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

type StringLockTarget struct {
	Components []StringLockTargetComponet `json:"components"`
}

func NewStringLockTarget() *StringLockTarget {
	return &StringLockTarget{}
}

// Add 添加一个Component，并将其内容设置为compValues
func (t *StringLockTarget) Add(compValues ...any) *StringLockTarget {
	t.Components = append(t.Components, StringLockTargetComponet{
		Values: lo.Map(compValues, func(val any, index int) string { return fmt.Sprintf("%v", val) }),
	})

	return t
}

// IsConflict 判断两个锁对象是否冲突。注：只有相同的结构的Target才有意义
func (t *StringLockTarget) IsConflict(other *StringLockTarget) bool {
	if len(t.Components) != len(other.Components) {
		return false
	}

	if len(t.Components) == 0 {
		return true
	}

	for i := 0; i < len(t.Components); i++ {
		if t.Components[i].IsEquals(&other.Components[i]) {
			return true
		}
	}

	return false
}

type StringLockTargetComponet struct {
	Values []string `json:"values"`
}

// IsEquals 判断两个Component是否相同。注：只有相同的结构的Component才有意义
func (t *StringLockTargetComponet) IsEquals(other *StringLockTargetComponet) bool {
	if len(t.Values) != len(other.Values) {
		return false
	}

	for i := 0; i < len(t.Values); i++ {
		if t.Values[i] != other.Values[i] {
			return false
		}
	}

	return true
}

func StringLockTargetToString(target *StringLockTarget) (string, error) {
	data, err := serder.ObjectToJSON(target)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func StringLockTargetFromString(str string) (StringLockTarget, error) {
	var ret StringLockTarget
	err := serder.JSONToObject([]byte(str), &ret)
	return ret, err
}
