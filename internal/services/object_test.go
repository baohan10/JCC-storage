package services

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

func Test_chooseUpdateRepObjectNode(t *testing.T) {
	testcases := []struct {
		title      string
		nodes      []coormsg.PreUpdateRepObjectRespNode
		wantNodeID int
	}{
		{
			title: "选择同地域，包含旧数据的节点",
			nodes: []coormsg.PreUpdateRepObjectRespNode{
				coormsg.NewPreUpdateRepObjectRespNode(0, "", "", true, false),
				coormsg.NewPreUpdateRepObjectRespNode(1, "", "", false, false),
				coormsg.NewPreUpdateRepObjectRespNode(2, "", "", false, true),
				coormsg.NewPreUpdateRepObjectRespNode(3, "", "", true, true),
			},
			wantNodeID: 3,
		},

		{
			title: "选择包含旧数据的节点",
			nodes: []coormsg.PreUpdateRepObjectRespNode{
				coormsg.NewPreUpdateRepObjectRespNode(0, "", "", true, false),
				coormsg.NewPreUpdateRepObjectRespNode(1, "", "", false, false),
				coormsg.NewPreUpdateRepObjectRespNode(2, "", "", false, true),
			},
			wantNodeID: 2,
		},

		{
			title: "选择包含同地域的节点",
			nodes: []coormsg.PreUpdateRepObjectRespNode{
				coormsg.NewPreUpdateRepObjectRespNode(0, "", "", true, false),
				coormsg.NewPreUpdateRepObjectRespNode(1, "", "", false, false),
			},
			wantNodeID: 0,
		},
	}

	var svc ObjectService
	for _, test := range testcases {
		Convey(test.title, t, func() {
			chooseNode := svc.chooseUpdateRepObjectNode(test.nodes)
			So(chooseNode.ID, ShouldEqual, test.wantNodeID)
		})
	}
}
