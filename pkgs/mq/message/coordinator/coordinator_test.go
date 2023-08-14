package coordinator

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gitlink.org.cn/cloudream/common/pkg/mq"
)

func TestSerder(t *testing.T) {
	Convey("序列化ReadCmd", t, func() {
		msg := mq.MakeMessage(NewPreDownloadObject(1, 123, ""))

		data, err := mq.Serialize(msg)

		So(err, ShouldBeNil)

		deMsg, err := mq.Deserialize(data)

		So(err, ShouldBeNil)

		So(*deMsg, ShouldResemble, msg)
	})
}
