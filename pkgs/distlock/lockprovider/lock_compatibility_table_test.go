package lockprovider

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
)

func Test_LockCompatibilityTable(t *testing.T) {
	Convey("兼容，互斥，特殊比较", t, func() {
		table := LockCompatibilityTable{}

		table.
			Column("l1", func() bool { return true }).
			Column("l2", func() bool { return true }).
			Column("l3", func() bool { return false })

		comp := LockCompatible()
		uncp := LockUncompatible()
		spcl := LockSpecial(func(lock distlock.Lock, testLockName string) bool { return true })
		table.Row(comp, comp, comp)
		table.Row(comp, uncp, comp)
		table.Row(comp, comp, spcl)

		err := table.Test(distlock.Lock{
			Name: "l1",
		})
		So(err, ShouldBeNil)

		err = table.Test(distlock.Lock{
			Name: "l2",
		})
		So(err, ShouldNotBeNil)

		err = table.Test(distlock.Lock{
			Name: "l3",
		})
		So(err, ShouldBeNil)
	})
}
