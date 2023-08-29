package lockprovider

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_StringLockTarget(t *testing.T) {
	cases := []struct {
		title          string
		target1        *StringLockTarget
		target2        *StringLockTarget
		wantIsConflict bool
	}{
		{
			title:          "没有任何段算冲突",
			target1:        NewStringLockTarget(),
			target2:        NewStringLockTarget(),
			wantIsConflict: true,
		},
		{
			title:          "有段，但段内为空，算冲突",
			target1:        NewStringLockTarget().Add(),
			target2:        NewStringLockTarget().Add(),
			wantIsConflict: true,
		},
		{
			title:          "每一段不同才不冲突",
			target1:        NewStringLockTarget().Add("a").Add("b"),
			target2:        NewStringLockTarget().Add("b").Add("c"),
			wantIsConflict: false,
		},
		{
			title:          "只要有一段相同就冲突",
			target1:        NewStringLockTarget().Add("a").Add("b"),
			target2:        NewStringLockTarget().Add("a").Add("c"),
			wantIsConflict: true,
		},
		{
			title:          "同段内，只要有一个数据不同就不冲突",
			target1:        NewStringLockTarget().Add("a", "b"),
			target2:        NewStringLockTarget().Add("b", "b"),
			wantIsConflict: false,
		},
		{
			title:          "同段内，只要每个数据都相同才不冲突",
			target1:        NewStringLockTarget().Add("a", "b"),
			target2:        NewStringLockTarget().Add("a", "b"),
			wantIsConflict: true,
		},
	}

	for _, ca := range cases {
		Convey(ca.title, t, func() {
			ret := ca.target1.IsConflict(ca.target2)
			So(ret, ShouldEqual, ca.wantIsConflict)
		})
	}
}
