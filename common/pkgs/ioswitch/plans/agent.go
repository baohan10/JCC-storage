package plans

import (
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"
)

type AgentPlanBuilder struct {
	blder *PlanBuilder
	node  cdssdk.Node
	ops   []ioswitch.Op
}

type AgentStreamVar struct {
	owner *AgentPlanBuilder
	v     *ioswitch.StreamVar
}

type AgentIntVar struct {
	owner *AgentPlanBuilder
	v     *ioswitch.IntVar
}

type AgentStringVar struct {
	owner *AgentPlanBuilder
	v     *ioswitch.StringVar
}

func (b *AgentPlanBuilder) IPFSRead(fileHash string, opts ...ipfs.ReadOption) *AgentStreamVar {
	opt := ipfs.ReadOption{
		Offset: 0,
		Length: -1,
	}
	if len(opts) > 0 {
		opt = opts[0]
	}

	str := &AgentStreamVar{
		owner: b,
		v:     b.blder.newStreamVar(),
	}

	b.ops = append(b.ops, &ops.IPFSRead{
		Output:   str.v,
		FileHash: fileHash,
		Option:   opt,
	})

	return str
}

func (s *AgentStreamVar) IPFSWrite() *AgentStringVar {
	v := s.owner.blder.newStringVar()

	s.owner.ops = append(s.owner.ops, &ops.IPFSWrite{
		Input:    s.v,
		FileHash: v,
	})

	return &AgentStringVar{
		owner: s.owner,
		v:     v,
	}
}

func (b *AgentPlanBuilder) FileRead(filePath string) *AgentStreamVar {
	agtStr := &AgentStreamVar{
		owner: b,
		v:     b.blder.newStreamVar(),
	}

	b.ops = append(b.ops, &ops.FileRead{
		Output:   agtStr.v,
		FilePath: filePath,
	})

	return agtStr
}

func (b *AgentStreamVar) FileWrite(filePath string) {
	b.owner.ops = append(b.owner.ops, &ops.FileWrite{
		Input:    b.v,
		FilePath: filePath,
	})
}

func (b *AgentPlanBuilder) ECReconstructAny(ec cdssdk.ECRedundancy, inBlockIndexes []int, outBlockIndexes []int, streams []*AgentStreamVar) []*AgentStreamVar {
	var strs []*AgentStreamVar

	var inputStrVars []*ioswitch.StreamVar
	for _, str := range streams {
		inputStrVars = append(inputStrVars, str.v)
	}

	var outputStrVars []*ioswitch.StreamVar
	for i := 0; i < len(outBlockIndexes); i++ {
		v := b.blder.newStreamVar()
		strs = append(strs, &AgentStreamVar{
			owner: b,
			v:     v,
		})
		outputStrVars = append(outputStrVars, v)
	}

	b.ops = append(b.ops, &ops.ECReconstructAny{
		EC:                 ec,
		Inputs:             inputStrVars,
		Outputs:            outputStrVars,
		InputBlockIndexes:  inBlockIndexes,
		OutputBlockIndexes: outBlockIndexes,
	})

	return strs
}

func (b *AgentPlanBuilder) ECReconstruct(ec cdssdk.ECRedundancy, inBlockIndexes []int, streams []*AgentStreamVar) []*AgentStreamVar {
	var strs []*AgentStreamVar

	var inputStrVars []*ioswitch.StreamVar
	for _, str := range streams {
		inputStrVars = append(inputStrVars, str.v)
	}

	var outputStrVars []*ioswitch.StreamVar
	for i := 0; i < ec.K; i++ {
		v := b.blder.newStreamVar()
		strs = append(strs, &AgentStreamVar{
			owner: b,
			v:     v,
		})
		outputStrVars = append(outputStrVars, v)
	}

	b.ops = append(b.ops, &ops.ECReconstruct{
		EC:                ec,
		Inputs:            inputStrVars,
		Outputs:           outputStrVars,
		InputBlockIndexes: inBlockIndexes,
	})

	return strs
}

func (b *AgentStreamVar) ChunkedSplit(chunkSize int, streamCount int, paddingZeros bool) []*AgentStreamVar {
	var strs []*AgentStreamVar

	var outputStrVars []*ioswitch.StreamVar
	for i := 0; i < streamCount; i++ {
		v := b.owner.blder.newStreamVar()
		strs = append(strs, &AgentStreamVar{
			owner: b.owner,
			v:     v,
		})
		outputStrVars = append(outputStrVars, v)
	}

	b.owner.ops = append(b.owner.ops, &ops.ChunkedSplit{
		Input:        b.v,
		Outputs:      outputStrVars,
		ChunkSize:    chunkSize,
		PaddingZeros: paddingZeros,
	})

	return strs
}

func (s *AgentStreamVar) Length(length int64) *AgentStreamVar {
	agtStr := &AgentStreamVar{
		owner: s.owner,
		v:     s.owner.blder.newStreamVar(),
	}

	s.owner.ops = append(s.owner.ops, &ops.Length{
		Input:  s.v,
		Output: agtStr.v,
		Length: length,
	})

	return agtStr
}

func (s *AgentStreamVar) To(node cdssdk.Node) *AgentStreamVar {
	s.owner.ops = append(s.owner.ops, &ops.SendStream{Stream: s.v, Node: node})
	s.owner = s.owner.blder.AtAgent(node)

	return s
}

func (s *AgentStreamVar) ToExecutor() *ExecutorStreamVar {
	s.owner.blder.executorPlan.ops = append(s.owner.blder.executorPlan.ops, &ops.GetStream{
		Stream: s.v,
		Node:   s.owner.node,
	})

	return &ExecutorStreamVar{
		blder: s.owner.blder,
		v:     s.v,
	}
}

func (b *AgentPlanBuilder) Join(length int64, streams []*AgentStreamVar) *AgentStreamVar {
	agtStr := &AgentStreamVar{
		owner: b,
		v:     b.blder.newStreamVar(),
	}

	var inputStrVars []*ioswitch.StreamVar
	for _, str := range streams {
		inputStrVars = append(inputStrVars, str.v)
	}

	b.ops = append(b.ops, &ops.Join{
		Inputs: inputStrVars,
		Output: agtStr.v,
		Length: length,
	})

	return agtStr
}

func (b *AgentPlanBuilder) ChunkedJoin(chunkSize int, streams []*AgentStreamVar) *AgentStreamVar {
	agtStr := &AgentStreamVar{
		owner: b,
		v:     b.blder.newStreamVar(),
	}

	var inputStrVars []*ioswitch.StreamVar
	for _, str := range streams {
		inputStrVars = append(inputStrVars, str.v)
	}

	b.ops = append(b.ops, &ops.ChunkedJoin{
		Inputs:    inputStrVars,
		Output:    agtStr.v,
		ChunkSize: chunkSize,
	})

	return agtStr
}

func (s *AgentStreamVar) Clone(cnt int) []*AgentStreamVar {
	var strs []*AgentStreamVar

	var outputStrVars []*ioswitch.StreamVar
	for i := 0; i < cnt; i++ {
		v := s.owner.blder.newStreamVar()
		strs = append(strs, &AgentStreamVar{
			owner: s.owner,
			v:     v,
		})
		outputStrVars = append(outputStrVars, v)
	}

	s.owner.ops = append(s.owner.ops, &ops.CloneStream{
		Input:   s.v,
		Outputs: outputStrVars,
	})

	return strs
}

func (v *AgentStringVar) To(node cdssdk.Node) *AgentStringVar {
	v.owner.ops = append(v.owner.ops, &ops.SendVar{Var: v.v, Node: node})
	v.owner = v.owner.blder.AtAgent(node)

	return v
}

func (v *AgentStringVar) ToExecutor() *ExecutorStringVar {
	v.owner.blder.executorPlan.ops = append(v.owner.blder.executorPlan.ops, &ops.GetVar{
		Var:  v.v,
		Node: v.owner.node,
	})

	return &ExecutorStringVar{
		blder: v.owner.blder,
		v:     v.v,
	}
}
