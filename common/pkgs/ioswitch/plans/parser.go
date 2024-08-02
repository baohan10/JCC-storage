package plans

import (
	"fmt"
	"math"

	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/common/utils/math2"
)

type FromToParser interface {
	Parse(ft FromTo, blder *PlanBuilder) error
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
	Ft      FromTo
	Nodes   []*Node
	ToNodes []*Node
	// 为了产生所有To所需的数据范围，而需要From打开的范围。
	// 这个范围是基于整个文件的，且上下界都取整到条带大小的整数倍，因此上界是有可能超过文件大小的。
	StreamRange Range
}

func (p *DefaultParser) Parse(ft FromTo, blder *PlanBuilder) error {
	ctx := ParseContext{Ft: ft}

	// 分成两个阶段：
	// 1. 基于From和To生成更多指令，初步匹配to的需求

	// 计算一下打开流的范围
	p.calcStreamRange(&ctx)

	err := p.extend(&ctx, ft, blder)
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
	// 从目前实现上来说不会死循环
	for {
		opted := false
		if p.pinIPFSRead(&ctx) {
			opted = true
		}
		if p.pinJoin(&ctx) {
			opted = true
		}
		if p.pinMultiply(&ctx) {
			opted = true
		}
		if p.pinSplit(&ctx) {
			opted = true
		}

		if !opted {
			break
		}
	}

	// 下面这些只需要执行一次，但需要按顺序
	p.dropUnused(&ctx)
	p.storeIPFSWriteResult(&ctx)
	p.generateClone(&ctx)
	p.generateRange(&ctx)
	p.generateSend(&ctx)

	return p.buildPlan(&ctx, blder)
}
func (p *DefaultParser) findOutputStream(ctx *ParseContext, dataIndex int) *StreamVar {
	for _, op := range ctx.Nodes {
		for _, o := range op.OutputStreams {
			if o.DataIndex == dataIndex {
				return o
			}
		}
	}

	return nil
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

func (p *DefaultParser) extend(ctx *ParseContext, ft FromTo, blder *PlanBuilder) error {
	for _, f := range ft.Froms {
		n, err := p.buildFromNode(ctx, &ft, f)
		if err != nil {
			return err
		}
		ctx.Nodes = append(ctx.Nodes, n)

		// 对于完整文件的From，生成Split指令
		if f.GetDataIndex() == -1 {
			splitOp := &Node{
				Env:  nil,
				Type: &ChunkedSplitOp{ChunkSize: p.EC.ChunkSize, PaddingZeros: true},
			}
			splitOp.AddInput(n.OutputStreams[0])
			for i := 0; i < p.EC.K; i++ {
				splitOp.NewOutput(i)
			}
			ctx.Nodes = append(ctx.Nodes, splitOp)
		}
	}

	// 如果有K个不同的文件块流，则生成Multiply指令，同时针对其生成的流，生成Join指令
	ecInputStrs := make(map[int]*StreamVar)
loop:
	for _, o := range ctx.Nodes {
		for _, s := range o.OutputStreams {
			if s.DataIndex >= 0 && ecInputStrs[s.DataIndex] == nil {
				ecInputStrs[s.DataIndex] = s
				if len(ecInputStrs) == p.EC.K {
					break loop
				}
			}
		}
	}
	if len(ecInputStrs) == p.EC.K {
		mulOp := &Node{
			Env:  nil,
			Type: &MultiplyOp{ChunkSize: p.EC.ChunkSize},
		}

		for _, s := range ecInputStrs {
			mulOp.AddInput(s)
		}
		for i := 0; i < p.EC.N; i++ {
			mulOp.NewOutput(i)
		}
		ctx.Nodes = append(ctx.Nodes, mulOp)

		joinOp := &Node{
			Env:  nil,
			Type: &ChunkedJoinOp{p.EC.ChunkSize},
		}
		for i := 0; i < p.EC.K; i++ {
			// 不可能找不到流
			joinOp.AddInput(p.findOutputStream(ctx, i))
		}
		joinOp.NewOutput(-1)
	}

	// 为每一个To找到一个输入流
	for _, t := range ft.Toes {
		n, err := p.buildToNode(&ft, t)
		if err != nil {
			return err
		}

		ctx.Nodes = append(ctx.Nodes, n)
		ctx.ToNodes = append(ctx.ToNodes, n)

		str := p.findOutputStream(ctx, t.GetDataIndex())
		if str == nil {
			return fmt.Errorf("no output stream found for data index %d", t.GetDataIndex())
		}

		n.AddInput(str)
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
	case *FromNode:
		ty := &IPFSReadType{
			FileHash: f.FileHash,
			Option: ipfs.ReadOption{
				Offset: 0,
				Length: -1,
			},
		}
		if f.DataIndex == -1 {
			ty.Option.Offset = repRange.Offset
			if repRange.Length != nil {
				ty.Option.Length = *repRange.Length
			}
		} else {
			ty.Option.Offset = blkRange.Offset
			if blkRange.Length != nil {
				ty.Option.Length = *blkRange.Length
			}
		}

		n := &Node{
			Type: ty,
		}
		n.NewOutput(f.DataIndex)

		if f.Node != nil {
			n.Env = &AgentEnv{Node: *f.Node}
		}

		return n, nil

	case *FromExecutor:
		n := &Node{
			Env:  &ExecutorEnv{},
			Type: &FromExecutorOp{Handle: f.Handle},
		}
		n.NewOutput(f.DataIndex)

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

func (p *DefaultParser) buildToNode(ft *FromTo, t To) (*Node, error) {
	switch t := t.(type) {
	case *ToNode:
		return &Node{
			Env:  &AgentEnv{t.Node},
			Type: &IPFSWriteType{FileHashStoreKey: t.FileHashStoreKey, Range: t.Range},
		}, nil

	case *ToExecutor:
		return &Node{
			Env:  &ExecutorEnv{},
			Type: &ToExecutorOp{Handle: t.Handle, Range: t.Range},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported to type %T", t)
	}
}

// 删除输出流未被使用的Join指令
func (p *DefaultParser) removeUnusedJoin(ctx *ParseContext) bool {
	opted := false
	for i, op := range ctx.Nodes {
		_, ok := op.Type.(*ChunkedJoinOp)
		if !ok {
			continue
		}

		if len(op.OutputStreams[0].Toes) > 0 {
			continue
		}

		for _, in := range op.InputStreams {
			in.RemoveTo(op)
		}

		ctx.Nodes[i] = nil
		opted = true
	}

	ctx.Nodes = lo2.RemoveAllDefault(ctx.Nodes)
	return opted
}

// 减少未使用的Multiply指令的输出流。如果减少到0，则删除该指令
func (p *DefaultParser) removeUnusedMultiplyOutput(ctx *ParseContext) bool {
	opted := false
	for i, op := range ctx.Nodes {
		_, ok := op.Type.(*MultiplyOp)
		if !ok {
			continue
		}

		for i2, out := range op.OutputStreams {
			if len(out.Toes) > 0 {
				continue
			}

			op.OutputStreams[i2] = nil
		}
		op.OutputStreams = lo2.RemoveAllDefault(op.OutputStreams)

		if len(op.OutputStreams) == 0 {
			for _, in := range op.InputStreams {
				in.RemoveTo(op)
			}

			ctx.Nodes[i] = nil
		}

		opted = true
	}

	ctx.Nodes = lo2.RemoveAllDefault(ctx.Nodes)
	return opted
}

// 删除未使用的Split指令
func (p *DefaultParser) removeUnusedSplit(ctx *ParseContext) bool {
	opted := false
	for i, op := range ctx.Nodes {
		_, ok := op.Type.(*ChunkedSplitOp)
		if !ok {
			continue
		}

		// Split出来的每一个流都没有被使用，才能删除这个指令
		isAllUnused := true
		for _, out := range op.OutputStreams {
			if len(out.Toes) > 0 {
				isAllUnused = false
				break
			}
		}

		if isAllUnused {
			op.InputStreams[0].RemoveTo(op)
			ctx.Nodes[i] = nil
			opted = true
		}
	}

	ctx.Nodes = lo2.RemoveAllDefault(ctx.Nodes)
	return opted
}

// 如果Split的结果被完全用于Join，则省略Split和Join指令
func (p *DefaultParser) omitSplitJoin(ctx *ParseContext) bool {
	opted := false
loop:
	for iSplit, splitOp := range ctx.Nodes {
		// 进行合并操作时会删除多个指令，因此这里存在splitOp == nil的情况
		if splitOp == nil {
			continue
		}

		_, ok := splitOp.Type.(*ChunkedSplitOp)
		if !ok {
			continue
		}

		// Split指令的每一个输出都有且只有一个目的地
		var joinOp *Node
		for _, out := range splitOp.OutputStreams {
			if len(out.Toes) != 1 {
				continue
			}

			if joinOp == nil {
				joinOp = out.Toes[0]
			} else if joinOp != out.Toes[0] {
				continue loop
			}
		}

		if joinOp == nil {
			continue
		}

		// 且这个目的地要是一个Join指令
		_, ok = joinOp.Type.(*ChunkedJoinOp)
		if !ok {
			continue
		}

		// 同时这个Join指令的输入也必须全部来自Split指令的输出。
		// 由于上面判断了Split指令的输出目的地都相同，所以这里只要判断Join指令的输入数量是否与Split指令的输出数量相同即可
		if len(joinOp.InputStreams) != len(splitOp.OutputStreams) {
			continue
		}

		// 所有条件都满足，可以开始省略操作，将Join操作的目的地的输入流替换为Split操作的输入流：
		// F->Split->Join->T 变换为：F->T
		splitOp.InputStreams[0].RemoveTo(splitOp)
		for _, to := range joinOp.OutputStreams[0].Toes {
			to.ReplaceInput(joinOp.OutputStreams[0], splitOp.InputStreams[0])
		}

		// 并删除这两个指令
		ctx.Nodes[iSplit] = nil
		lo2.Clear(ctx.Nodes, joinOp)
		opted = true
	}

	ctx.Nodes = lo2.RemoveAllDefault(ctx.Nodes)
	return opted
}

// 确定Split命令的执行位置
func (p *DefaultParser) pinSplit(ctx *ParseContext) bool {
	opted := false
	for _, op := range ctx.Nodes {
		_, ok := op.Type.(*ChunkedSplitOp)
		if !ok {
			continue
		}

		// 如果Split的每一个流的目的地都是同一个，则将Split固定在这个地方执行
		var toEnv OpEnv
		useToEnv := true
		for _, out := range op.OutputStreams {
			for _, to := range out.Toes {
				// 如果某个流的目的地也不确定，则将其视为与其他流的目的地相同
				if to.Env == nil {
					continue
				}

				if toEnv == nil {
					toEnv = to.Env
				} else if toEnv.Equals(to.Env) {
					useToEnv = false
					break
				}
			}
			if !useToEnv {
				break
			}
		}

		// 所有输出流的目的地都不确定，那么就不能根据输出流去固定
		if toEnv == nil {
			useToEnv = false
		}

		if useToEnv {
			if op.Env == nil || !op.Env.Equals(toEnv) {
				opted = true
			}

			op.Env = toEnv
			continue
		}

		// 此时查看输入流的始发地是否可以确定，可以的话使用这个位置
		fromEnv := op.InputStreams[0].From.Env
		if fromEnv != nil {
			if op.Env == nil || !op.Env.Equals(fromEnv) {
				opted = true
			}

			op.Env = fromEnv
		}
	}

	return opted
}

// 确定Join命令的执行位置，策略与固定Split类似
func (p *DefaultParser) pinJoin(ctx *ParseContext) bool {
	opted := false
	for _, op := range ctx.Nodes {
		_, ok := op.Type.(*ChunkedJoinOp)
		if !ok {
			continue
		}

		// 先查看输出流的目的地是否可以确定，可以的话使用这个位置
		var toEnv OpEnv
		for _, to := range op.OutputStreams[0].Toes {
			if to.Env == nil {
				continue
			}

			if toEnv == nil {
				toEnv = to.Env
			} else if !toEnv.Equals(to.Env) {
				toEnv = nil
				break
			}
		}

		if toEnv != nil {
			if op.Env == nil || !op.Env.Equals(toEnv) {
				opted = true
			}

			op.Env = toEnv
			continue
		}

		// 否则根据输入流的始发地来固定
		var fromEnv OpEnv
		for _, in := range op.InputStreams {
			if in.From.Env == nil {
				continue
			}

			if fromEnv == nil {
				fromEnv = in.From.Env
			} else if !fromEnv.Equals(in.From.Env) {
				// 输入流的始发地不同，那也必须选一个作为固定位置
				break
			}
		}

		// 所有输入流的始发地都不确定，那没办法了
		if fromEnv != nil {
			if op.Env == nil || !op.Env.Equals(fromEnv) {
				opted = true
			}

			op.Env = fromEnv
			continue
		}

	}

	return opted
}

// 确定Multiply命令的执行位置
func (p *DefaultParser) pinMultiply(ctx *ParseContext) bool {
	opted := false
	for _, op := range ctx.Nodes {
		_, ok := op.Type.(*MultiplyOp)
		if !ok {
			continue
		}

		var toEnv OpEnv
		for _, out := range op.OutputStreams {
			for _, to := range out.Toes {
				if to.Env == nil {
					continue
				}

				if toEnv == nil {
					toEnv = to.Env
				} else if !toEnv.Equals(to.Env) {
					toEnv = nil
					break
				}
			}
		}

		if toEnv != nil {
			if op.Env == nil || !op.Env.Equals(toEnv) {
				opted = true
			}

			op.Env = toEnv
			continue
		}

		// 否则根据输入流的始发地来固定
		var fromEnv OpEnv
		for _, in := range op.InputStreams {
			if in.From.Env == nil {
				continue
			}

			if fromEnv == nil {
				fromEnv = in.From.Env
			} else if !fromEnv.Equals(in.From.Env) {
				// 输入流的始发地不同，那也必须选一个作为固定位置
				break
			}
		}

		// 所有输入流的始发地都不确定，那没办法了
		if fromEnv != nil {
			if op.Env == nil || !op.Env.Equals(fromEnv) {
				opted = true
			}

			op.Env = fromEnv
			continue
		}

	}

	return opted
}

// 确定IPFS读取指令的执行位置
func (p *DefaultParser) pinIPFSRead(ctx *ParseContext) bool {
	opted := false
	for _, op := range ctx.Nodes {
		_, ok := op.Type.(*IPFSReadType)
		if !ok {
			continue
		}

		if op.Env != nil {
			continue
		}

		var toEnv OpEnv
		for _, to := range op.OutputStreams[0].Toes {
			if to.Env == nil {
				continue
			}

			if toEnv == nil {
				toEnv = to.Env
			} else if !toEnv.Equals(to.Env) {
				toEnv = nil
				break
			}
		}

		if toEnv != nil {
			if op.Env == nil || !op.Env.Equals(toEnv) {
				opted = true
			}

			op.Env = toEnv
		}
	}

	return opted
}

// 对于所有未使用的流，增加Drop指令
func (p *DefaultParser) dropUnused(ctx *ParseContext) {
	for _, op := range ctx.Nodes {
		for _, out := range op.OutputStreams {
			if len(out.Toes) == 0 {
				dropOp := &Node{
					Env:  nil,
					Type: &DropOp{},
				}
				dropOp.AddInput(out)
				ctx.Nodes = append(ctx.Nodes, dropOp)
			}
		}
	}
}

// 为IPFS写入指令存储结果
func (p *DefaultParser) storeIPFSWriteResult(ctx *ParseContext) {
	for _, op := range ctx.Nodes {
		w, ok := op.Type.(*IPFSWriteType)
		if !ok {
			continue
		}

		if w.FileHashStoreKey == "" {
			continue
		}

		storeOp := &Node{
			Env: &ExecutorEnv{},
			Type: &StoreOp{
				StoreKey: w.FileHashStoreKey,
			},
		}
		storeOp.AddInputVar(op.OutputValues[0])
		ctx.Nodes = append(ctx.Nodes, storeOp)
	}
}

// 生成Range指令。StreamRange可能超过文件总大小，但Range指令会在数据量不够时不报错而是正常返回
func (p *DefaultParser) generateRange(ctx *ParseContext) {
	for i, to := range ctx.ToNodes {
		toDataIdx := ctx.Ft.Toes[i].GetDataIndex()
		toRng := ctx.Ft.Toes[i].GetRange()

		if toDataIdx == -1 {
			rngType := &RangeType{Range: Range{Offset: toRng.Offset - ctx.StreamRange.Offset, Length: toRng.Length}}
			rngNode := &Node{
				Env:  to.InputStreams[0].From.Env,
				Type: rngType,
			}

			to.ReplaceInput(to.InputStreams[0], rngNode.NewOutput(toDataIdx))
		} else {
			stripSize := int64(p.EC.ChunkSize * p.EC.K)
			blkStartIdx := ctx.StreamRange.Offset / stripSize

			blkStart := blkStartIdx * int64(p.EC.ChunkSize)

			rngType := &RangeType{Range: Range{Offset: toRng.Offset - blkStart, Length: toRng.Length}}
			rngNode := &Node{
				Env:  to.InputStreams[0].From.Env,
				Type: rngType,
			}

			to.ReplaceInput(to.InputStreams[0], rngNode.NewOutput(toDataIdx))
		}
	}
}

// 生成Clone指令
func (p *DefaultParser) generateClone(ctx *ParseContext) {
	for _, op := range ctx.Nodes {
		for _, out := range op.OutputStreams {
			if len(out.Toes) <= 1 {
				continue
			}

			cloneOp := &Node{
				Env:  op.Env,
				Type: &CloneStreamOp{},
			}
			for _, to := range out.Toes {
				to.ReplaceInput(out, cloneOp.NewOutput(out.DataIndex))
			}
			out.Toes = nil
			cloneOp.AddInput(out)
			ctx.Nodes = append(ctx.Nodes, cloneOp)
		}

		for _, out := range op.OutputValues {
			if len(out.Toes) <= 1 {
				continue
			}

			cloneOp := &Node{
				Env:  op.Env,
				Type: &CloneVarOp{},
			}
			for _, to := range out.Toes {
				to.ReplaceInputVar(out, cloneOp.NewOutputVar(out.Type))
			}
			out.Toes = nil
			cloneOp.AddInputVar(out)
		}
	}
}

// 生成Send指令
func (p *DefaultParser) generateSend(ctx *ParseContext) {
	for _, op := range ctx.Nodes {
		for _, out := range op.OutputStreams {
			to := out.Toes[0]
			if to.Env.Equals(op.Env) {
				continue
			}

			switch to.Env.(type) {
			case *ExecutorEnv:
				// 如果是要送到Executor，则只能由Executor主动去拉取
				getStrOp := &Node{
					Env:  &ExecutorEnv{},
					Type: &GetStreamOp{},
				}
				out.Toes = nil
				getStrOp.AddInput(out)
				to.ReplaceInput(out, getStrOp.NewOutput(out.DataIndex))
				ctx.Nodes = append(ctx.Nodes, getStrOp)

			case *AgentEnv:
				// 如果是要送到Agent，则可以直接发送
				sendStrOp := &Node{
					Env:  op.Env,
					Type: &SendStreamOp{},
				}
				out.Toes = nil
				sendStrOp.AddInput(out)
				to.ReplaceInput(out, sendStrOp.NewOutput(out.DataIndex))
				ctx.Nodes = append(ctx.Nodes, sendStrOp)
			}
		}

		for _, out := range op.OutputValues {
			to := out.Toes[0]
			if to.Env.Equals(op.Env) {
				continue
			}

			switch to.Env.(type) {
			case *ExecutorEnv:
				// 如果是要送到Executor，则只能由Executor主动去拉取
				getVarOp := &Node{
					Env:  &ExecutorEnv{},
					Type: &GetVarOp{},
				}
				out.Toes = nil
				getVarOp.AddInputVar(out)
				to.ReplaceInputVar(out, getVarOp.NewOutputVar(out.Type))
				ctx.Nodes = append(ctx.Nodes, getVarOp)

			case *AgentEnv:
				// 如果是要送到Agent，则可以直接发送
				sendVarOp := &Node{
					Env:  op.Env,
					Type: &SendVarOp{},
				}
				out.Toes = nil
				sendVarOp.AddInputVar(out)
				to.ReplaceInputVar(out, sendVarOp.NewOutputVar(out.Type))
				ctx.Nodes = append(ctx.Nodes, sendVarOp)
			}
		}
	}
}

// 生成Plan
func (p *DefaultParser) buildPlan(ctx *ParseContext, blder *PlanBuilder) error {
	for _, op := range ctx.Nodes {
		for _, out := range op.OutputStreams {
			if out.Var != nil {
				continue
			}

			out.Var = blder.NewStreamVar()
		}

		for _, in := range op.InputStreams {
			if in.Var != nil {
				continue
			}

			in.Var = blder.NewStreamVar()
		}

		for _, out := range op.OutputValues {
			if out.Var != nil {
				continue
			}

			switch out.Type {
			case StringValueVar:
				out.Var = blder.NewStringVar()
			}

		}

		for _, in := range op.InputValues {
			if in.Var != nil {
				continue
			}

			switch in.Type {
			case StringValueVar:
				in.Var = blder.NewStringVar()
			}
		}

		if err := op.Type.GenerateOp(op, blder); err != nil {
			return err
		}
	}

	return nil
}
