package ec

import (
	"io"

	"github.com/klauspost/reedsolomon"
	"gitlink.org.cn/cloudream/common/utils/io2"
)

type StreamRs struct {
	encoder   reedsolomon.Encoder
	ecN       int
	ecK       int
	ecP       int
	chunkSize int
}

func NewStreamRs(k int, n int, chunkSize int) (*StreamRs, error) {
	enc := StreamRs{
		ecN:       n,
		ecK:       k,
		ecP:       n - k,
		chunkSize: chunkSize,
	}
	encoder, err := reedsolomon.New(k, n-k)
	enc.encoder = encoder
	return &enc, err
}

// 编码。仅输出校验块
func (r *StreamRs) Encode(input []io.Reader) []io.ReadCloser {
	outReaders := make([]io.ReadCloser, r.ecP)
	outWriters := make([]*io.PipeWriter, r.ecP)
	for i := 0; i < r.ecP; i++ {
		outReaders[i], outWriters[i] = io.Pipe()
	}

	go func() {
		chunks := make([][]byte, r.ecN)
		for idx := 0; idx < r.ecN; idx++ {
			chunks[idx] = make([]byte, r.chunkSize)
		}

		var closeErr error
	loop:
		for {
			//读块到buff
			for i := 0; i < r.ecK; i++ {
				_, err := io.ReadFull(input[i], chunks[i])
				if err != nil {
					closeErr = err
					break loop
				}
			}

			err := r.encoder.Encode(chunks)
			if err != nil {
				return
			}

			//输出到outWriter
			for i := range outWriters {
				err := io2.WriteAll(outWriters[i], chunks[i+r.ecK])
				if err != nil {
					closeErr = err
					break loop
				}
			}
		}

		for i := range outWriters {
			outWriters[i].CloseWithError(closeErr)
		}
	}()

	return outReaders
}

// 编码。输出包含所有的数据块和校验块
func (r *StreamRs) EncodeAll(input []io.Reader) []io.ReadCloser {
	outReaders := make([]io.ReadCloser, r.ecN)
	outWriters := make([]*io.PipeWriter, r.ecN)
	for i := 0; i < r.ecN; i++ {
		outReaders[i], outWriters[i] = io.Pipe()
	}

	go func() {
		chunks := make([][]byte, r.ecN)
		for idx := 0; idx < r.ecN; idx++ {
			chunks[idx] = make([]byte, r.chunkSize)
		}

		var closeErr error
	loop:
		for {
			//读块到buff
			for i := 0; i < r.ecK; i++ {
				_, err := io.ReadFull(input[i], chunks[i])
				if err != nil {
					closeErr = err
					break loop
				}
			}

			err := r.encoder.Encode(chunks)
			if err != nil {
				return
			}

			//输出到outWriter
			for i := range outWriters {
				err := io2.WriteAll(outWriters[i], chunks[i])
				if err != nil {
					closeErr = err
					break loop
				}
			}
		}

		for i := range outWriters {
			outWriters[i].CloseWithError(closeErr)
		}
	}()

	return outReaders
}

// 降级读，任意k个块恢复出所有原始的数据块。
func (r *StreamRs) ReconstructData(input []io.Reader, inBlockIdx []int) []io.ReadCloser {
	outIndexes := make([]int, r.ecK)
	for i := 0; i < r.ecK; i++ {
		outIndexes[i] = i
	}

	return r.ReconstructSome(input, inBlockIdx, outIndexes)
}

// 修复，任意k个块恢复指定的数据块。
// 调用者应该保证input的每一个流长度相同，且均为chunkSize的整数倍
func (r *StreamRs) ReconstructSome(input []io.Reader, inBlockIdx []int, outBlockIdx []int) []io.ReadCloser {
	outReaders := make([]io.ReadCloser, len(outBlockIdx))
	outWriters := make([]*io.PipeWriter, len(outBlockIdx))
	for i := 0; i < len(outBlockIdx); i++ {
		outReaders[i], outWriters[i] = io.Pipe()
	}

	go func() {
		chunks := make([][]byte, r.ecN)
		// 只初始化输入的buf，输出的buf在调用重建函数之后，会自动建立出来
		for _, idx := range inBlockIdx {
			chunks[idx] = make([]byte, r.chunkSize)
		}

		//outBools:要输出的若干块idx
		outBools := make([]bool, r.ecN)
		for _, idx := range outBlockIdx {
			outBools[idx] = true
		}

		inBools := make([]bool, r.ecN)
		for _, idx := range inBlockIdx {
			inBools[idx] = true
		}

		var closeErr error
	loop:
		for {
			//读块到buff
			for i := 0; i < r.ecK; i++ {
				_, err := io.ReadFull(input[i], chunks[inBlockIdx[i]])
				if err != nil {
					closeErr = err
					break loop
				}
			}

			err := r.encoder.ReconstructSome(chunks, outBools)
			if err != nil {
				return
			}

			//输出到outWriter
			for i := range outBlockIdx {
				err := io2.WriteAll(outWriters[i], chunks[outBlockIdx[i]])
				if err != nil {
					closeErr = err
					break loop
				}

				// 设置buf长度为0，cap不会受影响。注：如果一个块既是输入又是输出，那不能清空这个块
				if !inBools[outBlockIdx[i]] {
					chunks[outBlockIdx[i]] = chunks[outBlockIdx[i]][:0]
				}
			}
		}

		for i := range outWriters {
			outWriters[i].CloseWithError(closeErr)
		}
	}()

	return outReaders
}

// 重建任意块，包括数据块和校验块。
// 当前的实现会把不需要的块都重建出来，所以应该避免使用这个函数。
func (r *StreamRs) ReconstructAny(input []io.Reader, inBlockIdxes []int, outBlockIdxes []int) []io.ReadCloser {
	outReaders := make([]io.ReadCloser, len(outBlockIdxes))
	outWriters := make([]*io.PipeWriter, len(outBlockIdxes))
	for i := 0; i < len(outBlockIdxes); i++ {
		outReaders[i], outWriters[i] = io.Pipe()
	}

	go func() {
		chunks := make([][]byte, r.ecN)
		// 只初始化输入的buf，输出的buf在调用重建函数之后，会自动建立出来
		for _, idx := range inBlockIdxes {
			chunks[idx] = make([]byte, r.chunkSize)
		}

		//outBools:要输出的若干块idx
		outBools := make([]bool, r.ecN)
		for _, idx := range outBlockIdxes {
			outBools[idx] = true
		}

		inBools := make([]bool, r.ecN)
		for _, idx := range inBlockIdxes {
			inBools[idx] = true
		}

		var closeErr error
	loop:
		for {
			//读块到buff
			for i := 0; i < r.ecK; i++ {
				_, err := io.ReadFull(input[i], chunks[inBlockIdxes[i]])
				if err != nil {
					closeErr = err
					break loop
				}
			}

			err := r.encoder.Reconstruct(chunks)
			if err != nil {
				return
			}

			//输出到outWriter
			for i := range outBlockIdxes {
				outIndex := outBlockIdxes[i]

				err := io2.WriteAll(outWriters[i], chunks[outIndex])
				if err != nil {
					closeErr = err
					break loop
				}

				// 设置buf长度为0，cap不会受影响。注：如果一个块既是输入又是输出，那不能清空这个块
				if !inBools[outIndex] {
					chunks[outIndex] = chunks[outIndex][:0]
				}
			}
		}

		for i := range outWriters {
			outWriters[i].CloseWithError(closeErr)
		}
	}()

	return outReaders
}
