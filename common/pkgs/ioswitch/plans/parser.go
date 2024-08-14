package plans

import (
	"fmt"
	"math"

	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/exec"
)

type NodeProps struct {
	From From
	To   To
}

type ValueVarType int

const (
	StringValueVar ValueVarType = iota
	SignalValueVar
)

type VarProps struct {
	StreamIndex int          // 流的编号，只在StreamVar上有意义
	ValueType   ValueVarType // 值类型，只在ValueVar上有意义
	Var         ioswitch.Var // 生成Plan的时候创建的对应的Var
}

type Graph = dag.Graph[NodeProps, VarProps]

type Node = dag.Node[NodeProps, VarProps]

type StreamVar = dag.StreamVar[NodeProps, VarProps]

type ValueVar = dag.ValueVar[NodeProps, VarProps]

type AgentWorker struct {
	Node cdssdk.Node
}

func (w *AgentWorker) GetAddress() string {
	// TODO 选择地址
	return fmt.Sprintf("%v:%v", w.Node.ExternalIP, w.Node.ExternalGRPCPort)
}

func (w *AgentWorker) Equals(worker exec.Worker) bool {
	aw, ok := worker.(*AgentWorker)
	if !ok {
		return false
	}

	return w.Node.NodeID == aw.Node.NodeID
}

type FromToParser interface {
	Parse(ft FromTo, blder *builder.PlanBuilder) error
}

type DefaultParser struct {
	EC cdssdk.ECRedundancy
}

func NewParser(ec cdssdk.ECRedundancy) *DefaultParser {
	return &DefaultParser{
		EC: ec,
	}
}

type ParseContext struct {
	Ft  FromTo
	DAG *Graph
	// 为了产生所有To所需的数据范围，而需要From打开的范围。
	// 这个范围是基于整个文件的，且上下界都取整到条带大小的整数倍，因此上界是有可能超过文件大小的。
	StreamRange Range
}

func (p *DefaultParser) Parse(ft FromTo, blder *builder.PlanBuilder) error {
	ctx := ParseContext{Ft: ft}

	// 分成两个阶段：
	// 1. 基于From和To生成更多指令，初步匹配to的需求

	// 计算一下打开流的范围
	p.calcStreamRange(&ctx)

	err := p.extend(&ctx, ft)
	if err != nil {
		return err
	}

	// 2. 优化上一步生成的指令

	// 对于删除指令的优化，需要反复进行，直到没有变化为止。
	// 从目前实现上来说不会死循环
	for {
		opted := false
		if p.removeUnusedJoin(&ctx) {
			opted = true
		}
		if p.removeUnusedMultiplyOutput(&ctx) {
			opted = true
		}
		if p.removeUnusedSplit(&ctx) {
			opted = true
		}
		if p.omitSplitJoin(&ctx) {
			opted = true
		}

		if !opted {
			break
		}
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for p.pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	p.dropUnused(&ctx)
	p.storeIPFSWriteResult(&ctx)
	p.generateClone(&ctx)
	p.generateRange(&ctx)
	p.generateSend(&ctx)

	return p.buildPlan(&ctx, blder)
}
func (p *DefaultParser) findOutputStream(ctx *ParseContext, streamIndex int) *StreamVar {
	var ret *StreamVar
	ctx.DAG.Walk(func(n *dag.Node[NodeProps, VarProps]) bool {
		for _, o := range n.OutputStreams {
			if o != nil && o.Props.StreamIndex == streamIndex {
				ret = o
				return false
			}
		}
		return true
	})

	return ret
}

// 计算输入流的打开范围。会把流的范围按条带大小取整
func (p *DefaultParser) calcStreamRange(ctx *ParseContext) {
	stripSize := int64(p.EC.ChunkSize * p.EC.K)

	rng := Range{
		Offset: math.MaxInt64,
	}

	for _, to := range ctx.Ft.Toes {
		if to.GetDataIndex() == -1 {
			toRng := to.GetRange()
			rng.ExtendStart(math2.Floor(toRng.Offset, stripSize))
			if toRng.Length != nil {
				rng.ExtendEnd(math2.Ceil(toRng.Offset+*toRng.Length, stripSize))
			} else {
				rng.Length = nil
			}

		} else {
			toRng := to.GetRange()

			blkStartIndex := math2.FloorDiv(toRng.Offset, int64(p.EC.ChunkSize))
			rng.ExtendStart(blkStartIndex * stripSize)
			if toRng.Length != nil {
				blkEndIndex := math2.CeilDiv(toRng.Offset+*toRng.Length, int64(p.EC.ChunkSize))
				rng.ExtendEnd(blkEndIndex * stripSize)
			} else {
				rng.Length = nil
			}
		}
	}

	ctx.StreamRange = rng
}

func (p *DefaultParser) extend(ctx *ParseContext, ft FromTo) error {
	for _, f := range ft.Froms {
		_, err := p.buildFromNode(ctx, &ft, f)
		if err != nil {
			return err
		}

		// 对于完整文件的From，生成Split指令
		if f.GetDataIndex() == -1 {
			n, _ := dag.NewNode(ctx.DAG, &ChunkedSplitType{ChunkSize: p.EC.ChunkSize, OutputCount: p.EC.K}, NodeProps{})
			for i := 0; i < p.EC.K; i++ {
				n.OutputStreams[i].Props.StreamIndex = i
			}
		}
	}

	// 如果有K个不同的文件块流，则生成Multiply指令，同时针对其生成的流，生成Join指令
	ecInputStrs := make(map[int]*StreamVar)
loop:
	for _, o := range ctx.DAG.Nodes {
		for _, s := range o.OutputStreams {
			if s.Props.StreamIndex >= 0 && ecInputStrs[s.Props.StreamIndex] == nil {
				ecInputStrs[s.Props.StreamIndex] = s
				if len(ecInputStrs) == p.EC.K {
					break loop
				}
			}
		}
	}
	if len(ecInputStrs) == p.EC.K {
		mulNode, mulType := dag.NewNode(ctx.DAG, &MultiplyType{
			EC: p.EC,
		}, NodeProps{})

		for _, s := range ecInputStrs {
			mulType.AddInput(mulNode, s)
		}
		for i := 0; i < p.EC.N; i++ {
			mulType.NewOutput(mulNode, i)
		}

		joinNode, _ := dag.NewNode(ctx.DAG, &ChunkedJoinType{
			InputCount: p.EC.K,
			ChunkSize:  p.EC.ChunkSize,
		}, NodeProps{})

		for i := 0; i < p.EC.K; i++ {
			// 不可能找不到流
			p.findOutputStream(ctx, i).To(joinNode, i)
		}
		joinNode.OutputStreams[0].Props.StreamIndex = -1
	}

	// 为每一个To找到一个输入流
	for _, t := range ft.Toes {
		n, err := p.buildToNode(ctx, &ft, t)
		if err != nil {
			return err
		}

		str := p.findOutputStream(ctx, t.GetDataIndex())
		if str == nil {
			return fmt.Errorf("no output stream found for data index %d", t.GetDataIndex())
		}

		str.To(n, 0)
	}

	return nil
}

func (p *DefaultParser) buildFromNode(ctx *ParseContext, ft *FromTo, f From) (*Node, error) {
	var repRange Range
	var blkRange Range

	repRange.Offset = ctx.StreamRange.Offset
	blkRange.Offset = ctx.StreamRange.Offset / int64(p.EC.ChunkSize*p.EC.K) * int64(p.EC.ChunkSize)
	if ctx.StreamRange.Length != nil {
		repRngLen := *ctx.StreamRange.Length
		repRange.Length = &repRngLen

		blkRngLen := *ctx.StreamRange.Length / int64(p.EC.ChunkSize*p.EC.K) * int64(p.EC.ChunkSize)
		blkRange.Length = &blkRngLen
	}

	switch f := f.(type) {
	case *FromWorker:
		n, t := dag.NewNode(ctx.DAG, &IPFSReadType{
			FileHash: f.FileHash,
			Option: ipfs.ReadOption{
				Offset: 0,
				Length: -1,
			},
		}, NodeProps{
			From: f,
		})
		n.OutputStreams[0].Props.StreamIndex = f.DataIndex

		if f.DataIndex == -1 {
			t.Option.Offset = repRange.Offset
			if repRange.Length != nil {
				t.Option.Length = *repRange.Length
			}
		} else {
			t.Option.Offset = blkRange.Offset
			if blkRange.Length != nil {
				t.Option.Length = *blkRange.Length
			}
		}

		if f.Node != nil {
			n.Env.ToEnvWorker(&AgentWorker{*f.Node})
		}

		return n, nil

	case *FromExecutor:
		n, _ := dag.NewNode(ctx.DAG, &FromExecutorType{Handle: f.Handle}, NodeProps{From: f})
		n.Env.ToEnvExecutor()
		n.OutputStreams[0].Props.StreamIndex = f.DataIndex

		if f.DataIndex == -1 {
			f.Handle.RangeHint.Offset = repRange.Offset
			f.Handle.RangeHint.Length = repRange.Length
		} else {
			f.Handle.RangeHint.Offset = blkRange.Offset
			f.Handle.RangeHint.Length = blkRange.Length
		}

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported from type %T", f)
	}
}

func (p *DefaultParser) buildToNode(ctx *ParseContext, ft *FromTo, t To) (*Node, error) {
	switch t := t.(type) {
	case *ToNode:
		n, _ := dag.NewNode(ctx.DAG, &IPFSWriteType{
			FileHashStoreKey: t.FileHashStoreKey,
			Range:            t.Range,
		}, NodeProps{
			To: t,
		})

		return n, nil

	case *ToExecutor:
		n, _ := dag.NewNode(ctx.DAG, &ToExecutorType{Handle: t.Handle, Range: t.Range}, NodeProps{To: t})
		n.Env.ToEnvExecutor()

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported to type %T", t)
	}
}

// 删除输出流未被使用的Join指令
func (p *DefaultParser) removeUnusedJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ChunkedJoinType](ctx.DAG, func(node *Node, typ *ChunkedJoinType) bool {
		if len(node.OutputStreams[0].Toes) > 0 {
			return true
		}

		for _, in := range node.InputStreams {
			in.NotTo(node)
		}

		ctx.DAG.RemoveNode(node)
		return true
	})

	return changed
}

// 减少未使用的Multiply指令的输出流。如果减少到0，则删除该指令
func (p *DefaultParser) removeUnusedMultiplyOutput(ctx *ParseContext) bool {
	changed := false
	dag.WalkOnlyType[*MultiplyType](ctx.DAG, func(node *Node, typ *MultiplyType) bool {
		for i2, out := range node.OutputStreams {
			if len(out.Toes) > 0 {
				continue
			}

			node.OutputStreams[i2] = nil
			changed = true
		}
		node.OutputStreams = lo2.RemoveAllDefault(node.OutputStreams)

		// 如果所有输出流都被删除，则删除该指令
		if len(node.OutputStreams) == 0 {
			for _, in := range node.InputStreams {
				in.NotTo(node)
			}

			ctx.DAG.RemoveNode(node)
			changed = true
		}

		return true
	})
	return changed
}

// 删除未使用的Split指令
func (p *DefaultParser) removeUnusedSplit(ctx *ParseContext) bool {
	changed := false
	dag.WalkOnlyType[*ChunkedSplitType](ctx.DAG, func(node *Node, typ *ChunkedSplitType) bool {
		// Split出来的每一个流都没有被使用，才能删除这个指令
		for _, out := range node.OutputStreams {
			if len(out.Toes) > 0 {
				return true
			}
		}

		node.InputStreams[0].NotTo(node)
		ctx.DAG.RemoveNode(node)
		changed = true
		return true
	})

	return changed
}

// 如果Split的结果被完全用于Join，则省略Split和Join指令
func (p *DefaultParser) omitSplitJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ChunkedSplitType](ctx.DAG, func(splitNode *Node, typ *ChunkedSplitType) bool {
		// Split指令的每一个输出都有且只有一个目的地
		var joinNode *Node
		for _, out := range splitNode.OutputStreams {
			if len(out.Toes) != 1 {
				continue
			}

			if joinNode == nil {
				joinNode = out.Toes[0].Node
			} else if joinNode != out.Toes[0].Node {
				return true
			}
		}

		if joinNode == nil {
			return true
		}

		// 且这个目的地要是一个Join指令
		_, ok := joinNode.Type.(*ChunkedJoinType)
		if !ok {
			return true
		}

		// 同时这个Join指令的输入也必须全部来自Split指令的输出。
		// 由于上面判断了Split指令的输出目的地都相同，所以这里只要判断Join指令的输入数量是否与Split指令的输出数量相同即可
		if len(joinNode.InputStreams) != len(splitNode.OutputStreams) {
			return true
		}

		// 所有条件都满足，可以开始省略操作，将Join操作的目的地的输入流替换为Split操作的输入流：
		// F->Split->Join->T 变换为：F->T
		splitNode.InputStreams[0].NotTo(splitNode)
		for _, out := range joinNode.OutputStreams[0].Toes {
			splitNode.InputStreams[0].To(out.Node, out.SlotIndex)
		}

		// 并删除这两个指令
		ctx.DAG.RemoveNode(joinNode)
		ctx.DAG.RemoveNode(splitNode)

		changed = true
		return true
	})

	return changed
}

// 通过流的输入输出位置来确定指令的执行位置。
// To系列的指令都会有固定的执行位置，这些位置会随着pin操作逐步扩散到整个DAG，
// 所以理论上不会出现有指令的位置始终无法确定的情况。
func (p *DefaultParser) pin(ctx *ParseContext) bool {
	changed := false
	ctx.DAG.Walk(func(node *Node) bool {
		var toEnv *dag.NodeEnv
		for _, out := range node.OutputStreams {
			for _, to := range out.Toes {
				if to.Node.Env.Type == dag.EnvUnknown {
					continue
				}

				if toEnv == nil {
					toEnv = &to.Node.Env
				} else if !toEnv.Equals(to.Node.Env) {
					toEnv = nil
					break
				}
			}
		}

		if toEnv != nil {
			if !node.Env.Equals(*toEnv) {
				changed = true
			}

			node.Env = *toEnv
			return true
		}

		// 否则根据输入流的始发地来固定
		var fromEnv *dag.NodeEnv
		for _, in := range node.InputStreams {
			if in.From.Node.Env.Type == dag.EnvUnknown {
				continue
			}

			if fromEnv == nil {
				fromEnv = &in.From.Node.Env
			} else if !fromEnv.Equals(in.From.Node.Env) {
				fromEnv = nil
				break
			}
		}

		if fromEnv != nil {
			if !node.Env.Equals(*fromEnv) {
				changed = true
			}

			node.Env = *fromEnv
		}
		return true
	})

	return changed
}

// 对于所有未使用的流，增加Drop指令
func (p *DefaultParser) dropUnused(ctx *ParseContext) {
	ctx.DAG.Walk(func(node *Node) bool {
		for _, out := range node.OutputStreams {
			if len(out.Toes) == 0 {
				n := ctx.DAG.NewNode(&DropType{}, NodeProps{})
				n.Env = node.Env
				out.To(n, 0)
			}
		}
		return true
	})
}

// 为IPFS写入指令存储结果
func (p *DefaultParser) storeIPFSWriteResult(ctx *ParseContext) {
	dag.WalkOnlyType[*IPFSWriteType](ctx.DAG, func(node *Node, typ *IPFSWriteType) bool {
		if typ.FileHashStoreKey == "" {
			return true
		}

		n := ctx.DAG.NewNode(&StoreType{
			StoreKey: typ.FileHashStoreKey,
		}, NodeProps{})
		n.Env.ToEnvExecutor()

		node.OutputValues[0].To(n, 0)
		return true
	})
}

// 生成Range指令。StreamRange可能超过文件总大小，但Range指令会在数据量不够时不报错而是正常返回
func (p *DefaultParser) generateRange(ctx *ParseContext) {
	ctx.DAG.Walk(func(node *dag.Node[NodeProps, VarProps]) bool {
		if node.Props.To == nil {
			return true
		}

		toDataIdx := node.Props.To.GetDataIndex()
		toRng := node.Props.To.GetRange()

		if toDataIdx == -1 {
			n := ctx.DAG.NewNode(&RangeType{
				Range: Range{
					Offset: toRng.Offset - ctx.StreamRange.Offset,
					Length: toRng.Length,
				},
			}, NodeProps{})
			n.Env = node.InputStreams[0].From.Node.Env

			node.InputStreams[0].To(n, 0)
			node.InputStreams[0].NotTo(node)
			n.OutputStreams[0].To(node, 0)

		} else {
			stripSize := int64(p.EC.ChunkSize * p.EC.K)
			blkStartIdx := ctx.StreamRange.Offset / stripSize

			blkStart := blkStartIdx * int64(p.EC.ChunkSize)

			n := ctx.DAG.NewNode(&RangeType{
				Range: Range{
					Offset: toRng.Offset - blkStart,
					Length: toRng.Length,
				},
			}, NodeProps{})
			n.Env = node.InputStreams[0].From.Node.Env

			node.InputStreams[0].To(n, 0)
			node.InputStreams[0].NotTo(node)
			n.OutputStreams[0].To(node, 0)
		}

		return true
	})
}

// 生成Clone指令
func (p *DefaultParser) generateClone(ctx *ParseContext) {
	ctx.DAG.Walk(func(node *dag.Node[NodeProps, VarProps]) bool {
		for _, out := range node.OutputStreams {
			if len(out.Toes) <= 1 {
				continue
			}

			n, t := dag.NewNode(ctx.DAG, &CloneStreamType{}, NodeProps{})
			n.Env = node.Env
			for _, to := range out.Toes {
				t.NewOutput(node).To(to.Node, to.SlotIndex)
			}
			out.Toes = nil
			out.To(n, 0)
		}

		for _, out := range node.OutputValues {
			if len(out.Toes) <= 1 {
				continue
			}

			n, t := dag.NewNode(ctx.DAG, &CloneVarType{}, NodeProps{})
			n.Env = node.Env
			for _, to := range out.Toes {
				t.NewOutput(node).To(to.Node, to.SlotIndex)
			}
			out.Toes = nil
			out.To(n, 0)
		}

		return true
	})
}

// 生成Send指令
func (p *DefaultParser) generateSend(ctx *ParseContext) {
	ctx.DAG.Walk(func(node *dag.Node[NodeProps, VarProps]) bool {
		for _, out := range node.OutputStreams {
			to := out.Toes[0]
			if to.Node.Env.Equals(node.Env) {
				continue
			}

			switch to.Node.Env.Type {
			case dag.EnvExecutor:
				// // 如果是要送到Executor，则只能由Executor主动去拉取
				getNode := ctx.DAG.NewNode(&GetStreamType{}, NodeProps{})
				getNode.Env.ToEnvExecutor()

				// // 同时需要对此变量生成HoldUntil指令，避免Plan结束时Get指令还未到达
				holdNode := ctx.DAG.NewNode(&HoldUntilType{}, NodeProps{})
				holdNode.Env = node.Env

				// 将Get指令的信号送到Hold指令
				getNode.OutputValues[0].To(holdNode, 0)
				// 将Get指令的输出送到目的地
				getNode.OutputStreams[0].To(to.Node, to.SlotIndex)
				out.Toes = nil
				// 将源节点的输出送到Hold指令
				out.To(holdNode, 0)
				// 将Hold指令的输出送到Get指令
				holdNode.OutputStreams[0].To(getNode, 0)

			case dag.EnvWorker:
				// 如果是要送到Agent，则可以直接发送
				n := ctx.DAG.NewNode(&SendStreamType{}, NodeProps{})
				n.Env = node.Env
				n.OutputStreams[0].To(to.Node, to.SlotIndex)
				out.Toes = nil
				out.To(n, 0)
			}
		}

		for _, out := range node.OutputValues {
			to := out.Toes[0]
			if to.Node.Env.Equals(node.Env) {
				continue
			}

			switch to.Node.Env.Type {
			case dag.EnvExecutor:
				// // 如果是要送到Executor，则只能由Executor主动去拉取
				getNode := ctx.DAG.NewNode(&GetVaType{}, NodeProps{})
				getNode.Env.ToEnvExecutor()

				// // 同时需要对此变量生成HoldUntil指令，避免Plan结束时Get指令还未到达
				holdNode := ctx.DAG.NewNode(&HoldUntilType{}, NodeProps{})
				holdNode.Env = node.Env

				// 将Get指令的信号送到Hold指令
				getNode.OutputValues[0].To(holdNode, 0)
				// 将Get指令的输出送到目的地
				getNode.OutputValues[1].To(to.Node, to.SlotIndex)
				out.Toes = nil
				// 将源节点的输出送到Hold指令
				out.To(holdNode, 0)
				// 将Hold指令的输出送到Get指令
				holdNode.OutputValues[0].To(getNode, 0)

			case dag.EnvWorker:
				// 如果是要送到Agent，则可以直接发送
				n := ctx.DAG.NewNode(&SendVarType{}, NodeProps{})
				n.Env = node.Env
				n.OutputValues[0].To(to.Node, to.SlotIndex)
				out.Toes = nil
				out.To(n, 0)
			}
		}

		return true
	})
}

// 生成Plan
func (p *DefaultParser) buildPlan(ctx *ParseContext, blder *builder.PlanBuilder) error {
	var retErr error
	ctx.DAG.Walk(func(node *dag.Node[NodeProps, VarProps]) bool {
		for _, out := range node.OutputStreams {
			if out.Props.Var != nil {
				continue
			}

			out.Props.Var = blder.NewStreamVar()
		}

		for _, in := range node.InputStreams {
			if in.Props.Var != nil {
				continue
			}

			in.Props.Var = blder.NewStreamVar()
		}

		for _, out := range node.OutputValues {
			if out.Props.Var != nil {
				continue
			}

			switch out.Props.ValueType {
			case StringValueVar:
				out.Props.Var = blder.NewStringVar()
			case SignalValueVar:
				out.Props.Var = blder.NewSignalVar()
			}

		}

		for _, in := range node.InputValues {
			if in.Props.Var != nil {
				continue
			}

			switch in.Props.ValueType {
			case StringValueVar:
				in.Props.Var = blder.NewStringVar()
			case SignalValueVar:
				in.Props.Var = blder.NewSignalVar()
			}
		}

		if err := node.Type.GenerateOp(node, blder); err != nil {
			retErr = err
			return false
		}

		return true
	})

	return retErr
}
