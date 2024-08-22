package lrc

import "github.com/klauspost/reedsolomon"

type LRC struct {
	n      int   // 总块数，包括局部块
	k      int   // 数据块数量
	groups []int // 分组校验块生成时使用的数据块
	l      *reedsolomon.LRC
}

func New(n int, k int, groups []int) (*LRC, error) {
	lrc := &LRC{
		n:      n,
		k:      k,
		groups: groups,
	}

	l, err := reedsolomon.NewLRC(k, n-k, groups)
	if err != nil {
		return nil, err
	}

	lrc.l = l
	return lrc, nil
}

// 根据全局修复的原理，生成根据输入修复指定块的矩阵。要求input内元素的值<n-len(r)，且至少包含k个。
func (l *LRC) GenerateMatrix(inputIdxs []int, outputIdxs []int) ([][]byte, error) {
	return l.l.GenerateMatrix(inputIdxs, outputIdxs)
}

// 生成修复组内某个块的矩阵。只支持组内缺少一个块的情况，且默认组内的其他块都存在。
func (l *LRC) GenerateGroupMatrix(outputIdx int) ([][]byte, error) {
	return l.l.GenerateGroupMatrix(outputIdx)
}
