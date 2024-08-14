package dag

import (
	"gitlink.org.cn/cloudream/common/utils/lo2"
)

type Graph[NP any, VP any] struct {
	Nodes     []*Node[NP, VP]
	isWalking bool
	nextVarID int
}

func NewGraph[NP any, VP any]() *Graph[NP, VP] {
	return &Graph[NP, VP]{}
}

func (g *Graph[NP, VP]) NewNode(typ NodeType[NP, VP], props NP) *Node[NP, VP] {
	n := &Node[NP, VP]{
		Type:  typ,
		Props: props,
		Graph: g,
	}
	typ.InitNode(n)
	g.Nodes = append(g.Nodes, n)
	return n
}

func (g *Graph[NP, VP]) RemoveNode(node *Node[NP, VP]) {
	for i, n := range g.Nodes {
		if n == node {
			if g.isWalking {
				g.Nodes[i] = nil
			} else {
				g.Nodes = lo2.RemoveAt(g.Nodes, i)
			}
			break
		}
	}
}

func (g *Graph[NP, VP]) Walk(cb func(node *Node[NP, VP]) bool) {
	g.isWalking = true
	for i := 0; i < len(g.Nodes); i++ {
		if g.Nodes[i] == nil {
			continue
		}

		if !cb(g.Nodes[i]) {
			break
		}
	}
	g.isWalking = false

	g.Nodes = lo2.RemoveAllDefault(g.Nodes)
}

func (g *Graph[NP, VP]) genVarID() int {
	g.nextVarID++
	return g.nextVarID
}

func NewNode[NP any, VP any, NT NodeType[NP, VP]](graph *Graph[NP, VP], typ NT, props NP) (*Node[NP, VP], NT) {
	return graph.NewNode(typ, props), typ
}

func WalkOnlyType[N NodeType[NP, VP], NP any, VP any](g *Graph[NP, VP], cb func(node *Node[NP, VP], typ N) bool) {
	g.Walk(func(node *Node[NP, VP]) bool {
		typ, ok := node.Type.(N)
		if ok {
			return cb(node, typ)
		}
		return true
	})
}
