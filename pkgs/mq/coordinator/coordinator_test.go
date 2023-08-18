package coordinator

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSerder(t *testing.T) {
	Convey("输出注册的Handler", t, func() {
		for k, _ := range msgDispatcher.Handlers {
			t.Logf("(%s)", k)
		}
	})
}
