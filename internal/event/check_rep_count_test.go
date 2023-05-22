package event

import (
	"testing"

	"github.com/samber/lo"
	. "github.com/smartystreets/goconvey/convey"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/utils/sort"
	"gitlink.org.cn/cloudream/db/model"
)

func Test_chooseNewRepNodes(t *testing.T) {
	testcases := []struct {
		title       string
		allNodes    []model.Node
		curRepNodes []model.Node
		newCount    int
		wantNodeIDs []int
	}{
		{
			title: "优先选择不同地域的节点",
			allNodes: []model.Node{
				{
					NodeID:     1,
					LocationID: 1,
					State:      consts.NODE_STATE_NORMAL,
				},
				{
					NodeID:     2,
					LocationID: 1,
					State:      consts.NODE_STATE_NORMAL,
				},
				{
					NodeID:     3,
					LocationID: 2,
					State:      consts.NODE_STATE_NORMAL,
				},
				{
					NodeID:     4,
					LocationID: 3,
					State:      consts.NODE_STATE_NORMAL,
				},
			},
			curRepNodes: []model.Node{
				{
					NodeID:     1,
					LocationID: 1,
				},
			},
			newCount:    2,
			wantNodeIDs: []int{3, 4},
		},
		{
			title: "就算节点数不足，也不能选择重复节点",
			allNodes: []model.Node{
				{
					NodeID:     1,
					LocationID: 1,
					State:      consts.NODE_STATE_NORMAL,
				},
				{
					NodeID:     2,
					LocationID: 1,
					State:      consts.NODE_STATE_NORMAL,
				},
			},
			curRepNodes: []model.Node{
				{
					NodeID:     1,
					LocationID: 1,
				},
			},
			newCount:    2,
			wantNodeIDs: []int{2},
		},
		{
			title: "就算节点数不足，也不能选择状态unavailable的节点",
			allNodes: []model.Node{
				{
					NodeID:     1,
					LocationID: 1,
					State:      consts.NODE_STATE_UNAVAILABLE,
				},
				{
					NodeID:     2,
					LocationID: 1,
					State:      consts.NODE_STATE_NORMAL,
				},
			},
			curRepNodes: []model.Node{
				{
					NodeID:     3,
					LocationID: 1,
				},
			},
			newCount:    2,
			wantNodeIDs: []int{2},
		},
	}

	for _, test := range testcases {
		Convey(test.title, t, func() {
			chooseNodes := chooseNewRepNodes(test.allNodes, test.curRepNodes, test.newCount)
			chooseNodeIDs := lo.Map(chooseNodes, func(node model.Node, index int) int { return node.NodeID })

			sort.Sort(chooseNodeIDs, sort.Cmp[int])

			So(chooseNodeIDs, ShouldResemble, test.wantNodeIDs)
		})
	}
}

func Test_chooseDeleteAvaiRepNodes(t *testing.T) {
	testcases := []struct {
		title               string
		allNodes            []model.Node
		curRepNodes         []model.Node
		delCount            int
		wantNodeLocationIDs []int
	}{
		{
			title:    "优先选择地域重复的节点",
			allNodes: []model.Node{},
			curRepNodes: []model.Node{
				{NodeID: 1, LocationID: 1}, {NodeID: 2, LocationID: 1},
				{NodeID: 3, LocationID: 2}, {NodeID: 4, LocationID: 2},
				{NodeID: 5, LocationID: 3}, {NodeID: 6, LocationID: 3}, {NodeID: 7, LocationID: 3},
				{NodeID: 8, LocationID: 4},
			},
			delCount:            4,
			wantNodeLocationIDs: []int{1, 2, 3, 3},
		},
		{
			title:    "节点不够删",
			allNodes: []model.Node{},
			curRepNodes: []model.Node{
				{NodeID: 1, LocationID: 1},
			},
			delCount:            2,
			wantNodeLocationIDs: []int{1},
		},
	}

	for _, test := range testcases {
		Convey(test.title, t, func() {
			chooseNodes := chooseDeleteAvaiRepNodes(test.allNodes, test.curRepNodes, test.delCount)
			chooseNodeLocationIDs := lo.Map(chooseNodes, func(node model.Node, index int) int { return node.LocationID })

			sort.Sort(chooseNodeLocationIDs, sort.Cmp[int])

			So(chooseNodeLocationIDs, ShouldResemble, test.wantNodeLocationIDs)
		})
	}
}
