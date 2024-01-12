package event

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func newTreeTest(nodeBlocksMap []bitmap) combinatorialTree {
	tree := combinatorialTree{
		blocksMaps:          make(map[int]bitmap),
		nodeIDToLocalNodeID: make(map[cdssdk.NodeID]int),
	}

	tree.nodes = make([]combinatorialTreeNode, (1 << len(nodeBlocksMap)))
	for id, mp := range nodeBlocksMap {
		tree.nodeIDToLocalNodeID[cdssdk.NodeID(id)] = len(tree.localNodeIDToNodeID)
		tree.blocksMaps[len(tree.localNodeIDToNodeID)] = mp
		tree.localNodeIDToNodeID = append(tree.localNodeIDToNodeID, cdssdk.NodeID(id))
	}

	tree.nodes[0].localNodeID = -1
	index := 1
	tree.initNode(0, &tree.nodes[0], &index)

	return tree
}

func Test_iterCombBits(t *testing.T) {
	testcases := []struct {
		title          string
		width          int
		count          int
		expectedValues []int
	}{
		{
			title:          "1 of 4",
			width:          4,
			count:          1,
			expectedValues: []int{16, 8, 4, 2},
		},

		{
			title:          "2 of 4",
			width:          4,
			count:          2,
			expectedValues: []int{24, 20, 18, 12, 10, 6},
		},

		{
			title:          "3 of 4",
			width:          4,
			count:          3,
			expectedValues: []int{28, 26, 22, 14},
		},

		{
			title:          "4 of 4",
			width:          4,
			count:          4,
			expectedValues: []int{30},
		},
	}

	for _, test := range testcases {
		Convey(test.title, t, func() {
			var ret []int
			var t combinatorialTree
			t.iterCombBits(test.width, test.count, 0, func(i int) {
				ret = append(ret, i)
			})

			So(ret, ShouldResemble, test.expectedValues)
		})
	}
}

func Test_newCombinatorialTree(t *testing.T) {
	testcases := []struct {
		title                    string
		nodeBlocks               []bitmap
		expectedTreeNodeLocalIDs []int
		expectedTreeNodeBitmaps  []int
	}{
		{
			title:                    "1个节点",
			nodeBlocks:               []bitmap{1},
			expectedTreeNodeLocalIDs: []int{-1, 0},
			expectedTreeNodeBitmaps:  []int{0, 1},
		},
		{
			title:                    "2个节点",
			nodeBlocks:               []bitmap{1, 0},
			expectedTreeNodeLocalIDs: []int{-1, 0, 1, 1},
			expectedTreeNodeBitmaps:  []int{0, 1, 1, 0},
		},
		{
			title:                    "4个节点",
			nodeBlocks:               []bitmap{1, 2, 4, 8},
			expectedTreeNodeLocalIDs: []int{-1, 0, 1, 2, 3, 3, 2, 3, 3, 1, 2, 3, 3, 2, 3, 3},
			expectedTreeNodeBitmaps:  []int{0, 1, 3, 7, 15, 11, 5, 13, 9, 2, 6, 14, 10, 4, 12, 8},
		},
	}

	for _, test := range testcases {
		Convey(test.title, t, func() {
			t := newTreeTest(test.nodeBlocks)

			var localIDs []int
			var bitmaps []int
			for _, n := range t.nodes {
				localIDs = append(localIDs, n.localNodeID)
				bitmaps = append(bitmaps, int(n.blocksBitmap))
			}

			So(localIDs, ShouldResemble, test.expectedTreeNodeLocalIDs)
			So(bitmaps, ShouldResemble, test.expectedTreeNodeBitmaps)
		})
	}
}

func Test_UpdateBitmap(t *testing.T) {
	testcases := []struct {
		title                   string
		nodeBlocks              []bitmap
		updatedNodeID           cdssdk.NodeID
		updatedBitmap           bitmap
		k                       int
		expectedTreeNodeBitmaps []int
	}{

		{
			title:                   "4个节点，更新但值不变",
			nodeBlocks:              []bitmap{1, 2, 4, 8},
			updatedNodeID:           cdssdk.NodeID(0),
			updatedBitmap:           bitmap(1),
			k:                       4,
			expectedTreeNodeBitmaps: []int{0, 1, 3, 7, 15, 11, 5, 13, 9, 2, 6, 14, 10, 4, 12, 8},
		},

		{
			title:                   "4个节点，更新0",
			nodeBlocks:              []bitmap{1, 2, 4, 8},
			updatedNodeID:           cdssdk.NodeID(0),
			updatedBitmap:           bitmap(2),
			k:                       4,
			expectedTreeNodeBitmaps: []int{0, 2, 2, 6, 14, 10, 6, 14, 10, 2, 6, 14, 10, 4, 12, 8},
		},

		{
			title:                   "4个节点，更新1",
			nodeBlocks:              []bitmap{1, 2, 4, 8},
			updatedNodeID:           cdssdk.NodeID(1),
			updatedBitmap:           bitmap(1),
			k:                       4,
			expectedTreeNodeBitmaps: []int{0, 1, 1, 5, 13, 9, 5, 13, 9, 1, 5, 13, 9, 4, 12, 8},
		},

		{
			title:                   "4个节点，更新2",
			nodeBlocks:              []bitmap{1, 2, 4, 8},
			updatedNodeID:           cdssdk.NodeID(2),
			updatedBitmap:           bitmap(1),
			k:                       4,
			expectedTreeNodeBitmaps: []int{0, 1, 3, 3, 11, 11, 1, 9, 9, 2, 3, 11, 10, 1, 9, 8},
		},

		{
			title:                   "4个节点，更新3",
			nodeBlocks:              []bitmap{1, 2, 4, 8},
			updatedNodeID:           cdssdk.NodeID(3),
			updatedBitmap:           bitmap(1),
			k:                       4,
			expectedTreeNodeBitmaps: []int{0, 1, 3, 7, 7, 3, 5, 5, 1, 2, 6, 7, 3, 4, 5, 1},
		},

		{
			title:                   "4个节点，k<4，更新0，0之前没有k个块，现在拥有",
			nodeBlocks:              []bitmap{1, 2, 4, 8},
			updatedNodeID:           cdssdk.NodeID(0),
			updatedBitmap:           bitmap(3),
			k:                       2,
			expectedTreeNodeBitmaps: []int{0, 3, 3, 7, 15, 11, 5, 13, 9, 2, 6, 14, 10, 4, 12, 8},
		},
		{
			title:                   "4个节点，k<4，更新0，0之前有k个块，现在没有",
			nodeBlocks:              []bitmap{3, 4, 0, 0},
			updatedNodeID:           cdssdk.NodeID(0),
			updatedBitmap:           bitmap(0),
			k:                       2,
			expectedTreeNodeBitmaps: []int{0, 0, 4, 4, 4, 4, 0, 0, 0, 4, 4, 4, 4, 0, 0, 0},
		},
	}

	for _, test := range testcases {
		Convey(test.title, t, func() {
			t := newTreeTest(test.nodeBlocks)
			t.UpdateBitmap(test.updatedNodeID, test.updatedBitmap, test.k)

			var bitmaps []int
			for _, n := range t.nodes {
				bitmaps = append(bitmaps, int(n.blocksBitmap))
			}

			So(bitmaps, ShouldResemble, test.expectedTreeNodeBitmaps)
		})
	}
}

func Test_FindKBlocksMaxDepth(t *testing.T) {
	testcases := []struct {
		title      string
		nodeBlocks []bitmap
		k          int
		expected   int
	}{
		{
			title:      "每个节点各有一个块",
			nodeBlocks: []bitmap{1, 2, 4, 8},
			k:          2,
			expected:   2,
		},
		{
			title:      "所有节点加起来块数不足",
			nodeBlocks: []bitmap{1, 1, 1, 1},
			k:          2,
			expected:   4,
		},
		{
			title:      "不同节点有相同块",
			nodeBlocks: []bitmap{1, 1, 2, 4},
			k:          2,
			expected:   3,
		},
		{
			title:      "一个节点就拥有所有块",
			nodeBlocks: []bitmap{3, 6, 12, 24},
			k:          2,
			expected:   1,
		},
		{
			title:      "只有一块，且只在某一个节点1",
			nodeBlocks: []bitmap{1, 0},
			k:          1,
			expected:   2,
		},
		{
			title:      "只有一块，且只在某一个节点2",
			nodeBlocks: []bitmap{0, 1},
			k:          1,
			expected:   2,
		},
	}

	for _, test := range testcases {
		Convey(test.title, t, func() {
			t := newTreeTest(test.nodeBlocks)
			ret := t.FindKBlocksMaxDepth(test.k)
			So(ret, ShouldResemble, test.expected)
		})
	}
}
