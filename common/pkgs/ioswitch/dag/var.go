package dag

import "gitlink.org.cn/cloudream/common/utils/lo2"

type EndPoint[NP any, VP any] struct {
	Node      *Node[NP, VP]
	SlotIndex int // 所连接的Node的Output或Input数组的索引
}

type StreamVar[NP any, VP any] struct {
	ID    int
	From  EndPoint[NP, VP]
	Toes  []EndPoint[NP, VP]
	Props VP
}

func (v *StreamVar[NP, VP]) To(to *Node[NP, VP], slotIdx int) int {
	v.Toes = append(v.Toes, EndPoint[NP, VP]{Node: to, SlotIndex: slotIdx})
	to.InputStreams[slotIdx] = v
	return len(v.Toes) - 1
}

// func (v *StreamVar[NP, VP]) NotTo(toIdx int) EndPoint[NP, VP] {
// 	ed := v.Toes[toIdx]
// 	lo2.RemoveAt(v.Toes, toIdx)
// 	ed.Node.InputStreams[ed.SlotIndex] = nil
// 	return ed
// }

func (v *StreamVar[NP, VP]) NotTo(node *Node[NP, VP]) (EndPoint[NP, VP], bool) {
	for i, ed := range v.Toes {
		if ed.Node == node {
			v.Toes = lo2.RemoveAt(v.Toes, i)
			ed.Node.InputStreams[ed.SlotIndex] = nil
			return ed, true
		}
	}

	return EndPoint[NP, VP]{}, false
}

func (v *StreamVar[NP, VP]) NotToWhere(pred func(to EndPoint[NP, VP]) bool) []EndPoint[NP, VP] {
	var newToes []EndPoint[NP, VP]
	var rmed []EndPoint[NP, VP]
	for _, ed := range v.Toes {
		if pred(ed) {
			ed.Node.InputStreams[ed.SlotIndex] = nil
			rmed = append(rmed, ed)
		} else {
			newToes = append(newToes, ed)
		}
	}
	v.Toes = newToes
	return rmed
}

func (v *StreamVar[NP, VP]) NotToAll() []EndPoint[NP, VP] {
	for _, ed := range v.Toes {
		ed.Node.InputStreams[ed.SlotIndex] = nil
	}
	toes := v.Toes
	v.Toes = nil
	return toes
}

func NodeNewOutputStream[NP any, VP any](node *Node[NP, VP], props VP) *StreamVar[NP, VP] {
	str := &StreamVar[NP, VP]{
		ID:    node.Graph.genVarID(),
		From:  EndPoint[NP, VP]{Node: node, SlotIndex: len(node.OutputStreams)},
		Props: props,
	}
	node.OutputStreams = append(node.OutputStreams, str)
	return str
}

func NodeDeclareInputStream[NP any, VP any](node *Node[NP, VP], cnt int) {
	node.InputStreams = make([]*StreamVar[NP, VP], cnt)
}

type ValueVarType int

const (
	StringValueVar ValueVarType = iota
	SignalValueVar
)

type ValueVar[NP any, VP any] struct {
	ID    int
	From  EndPoint[NP, VP]
	Toes  []EndPoint[NP, VP]
	Props VP
}

func (v *ValueVar[NP, VP]) To(to *Node[NP, VP], slotIdx int) int {
	v.Toes = append(v.Toes, EndPoint[NP, VP]{Node: to, SlotIndex: slotIdx})
	to.InputValues[slotIdx] = v
	return len(v.Toes) - 1
}

func NodeNewOutputValue[NP any, VP any](node *Node[NP, VP], props VP) *ValueVar[NP, VP] {
	val := &ValueVar[NP, VP]{
		ID:    node.Graph.genVarID(),
		From:  EndPoint[NP, VP]{Node: node, SlotIndex: len(node.OutputStreams)},
		Props: props,
	}
	node.OutputValues = append(node.OutputValues, val)
	return val
}

func NodeDeclareInputValue[NP any, VP any](node *Node[NP, VP], cnt int) {
	node.InputValues = make([]*ValueVar[NP, VP], cnt)
}
