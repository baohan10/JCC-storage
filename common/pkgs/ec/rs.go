package ec

import (
	"github.com/klauspost/reedsolomon"
)

type Rs struct {
	encoder reedsolomon.Encoder
	ecN     int
	ecK     int
	ecP     int
}

func NewRs(k int, n int) (*Rs, error) {
	enc := Rs{
		ecN: n,
		ecK: k,
		ecP: n - k,
	}
	encoder, err := reedsolomon.New(k, n-k)
	enc.encoder = encoder
	return &enc, err
}

// 任意k个块恢复出所有原始的数据块。
// blocks的长度必须为N，且至少有K个元素不为nil
func (r *Rs) ReconstructData(blocks [][]byte) error {
	outIndexes := make([]int, r.ecK)
	for i := 0; i < r.ecK; i++ {
		outIndexes[i] = i
	}

	return r.ReconstructAny(blocks, outIndexes)
}

// 重建指定的任意块，可以是数据块或校验块。
// 在input上原地重建，因此input的长度必须为N。
func (r *Rs) ReconstructAny(blocks [][]byte, outBlockIdxes []int) error {
	required := make([]bool, len(blocks))
	for _, idx := range outBlockIdxes {
		required[idx] = true
	}
	return r.encoder.ReconstructAny(blocks, required)
}
