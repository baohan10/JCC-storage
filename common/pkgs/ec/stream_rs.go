package ec

import (
	"io"

	"github.com/klauspost/reedsolomon"
)

type Rs struct {
	encoder   reedsolomon.Encoder
	ecN       int
	ecK       int
	ecP       int
	chunkSize int64
}

func NewRs(k int, n int, chunkSize int64) (*Rs, error) {
	enc := Rs{
		ecN:       n,
		ecK:       k,
		ecP:       n - k,
		chunkSize: chunkSize,
	}
	encoder, err := reedsolomon.New(k, n-k)
	enc.encoder = encoder
	return &enc, err
}

// 编码
func (r *Rs) Encode(data []io.ReadCloser) ([]io.ReadCloser, error) {
	output := make([]io.ReadCloser, r.ecP)
	parity := make([]*io.PipeWriter, r.ecP)
	for i := range output {
		var reader *io.PipeReader
		reader, parity[i] = io.Pipe()
		output[i] = reader
	}
	go func() {
		chunks := make([][]byte, r.ecN)
		for i := range chunks {
			chunks[i] = make([]byte, r.chunkSize)
		}
		for {
			finished := false
			//读数据块到buff
			for i := 0; i < r.ecK; i++ {
				_, err := data[i].Read(chunks[i])
				if err != nil {
					finished = true
					break
				}
			}
			if finished {
				break
			}
			//编码
			err := r.encoder.Encode(chunks)
			if err != nil {
				return
			}
			//输出到writer
			for i := r.ecK; i < r.ecN; i++ {
				parity[i-r.ecK].Write(chunks[i])
			}
		}
		for i := range data {
			data[i].Close()
		}
		for i := range parity {
			parity[i].Close()
		}
	}()
	return output, nil
}

// 降级读，任意k个块恢复出原始数据块
func (r *Rs) ReconstructData(input []io.ReadCloser, inBlockIdx []int) ([]io.ReadCloser, error) {
	dataReader := make([]io.ReadCloser, r.ecK)
	dataWriter := make([]*io.PipeWriter, r.ecK)
	for i := 0; i < r.ecK; i++ {
		var reader *io.PipeReader
		reader, dataWriter[i] = io.Pipe()
		dataReader[i] = reader
	}
	go func() {
		chunks := make([][]byte, r.ecN)
		for i := range chunks {
			chunks[i] = make([]byte, r.chunkSize)
		}
		constructIdx := make([]bool, r.ecN)
		for i := 0; i < r.ecN; i++ {
			constructIdx[i] = false
		}
		for i := 0; i < r.ecK; i++ {
			constructIdx[inBlockIdx[i]] = true
		}
		nilIdx := make([]int, r.ecP)
		ct := 0
		for i := 0; i < r.ecN; i++ {
			if !constructIdx[i] {
				nilIdx[ct] = i
				ct++
			}
		}

		for {
			finished := false

			//读数据块到buff
			for i := 0; i < r.ecK; i++ {
				_, err := input[i].Read(chunks[inBlockIdx[i]])
				if err != nil {
					finished = true
					break
				}
			}
			for i := 0; i < r.ecP; i++ {
				chunks[nilIdx[i]] = nil
			}
			if finished {
				break
			}
			//解码
			err := r.encoder.ReconstructData(chunks)
			if err != nil {
				return
			}
			//输出到writer
			for i := 0; i < r.ecK; i++ {
				dataWriter[i].Write(chunks[i])
			}
		}
		for i := range input {
			input[i].Close()
		}
		for i := range dataWriter {
			dataWriter[i].Close()
		}
	}()
	return dataReader, nil
}

// 修复，任意k个块恢复若干想要的块
func (r *Rs) ReconstructSome(input []io.ReadCloser, inBlockIdx []int, outBlockIdx []int) ([]io.ReadCloser, error) {
	outReader := make([]io.ReadCloser, len(outBlockIdx))
	outWriter := make([]*io.PipeWriter, len(outBlockIdx))
	for i := 0; i < len(outBlockIdx); i++ {
		var reader *io.PipeReader
		reader, outWriter[i] = io.Pipe()
		outReader[i] = reader
	}
	go func() {
		chunks := make([][]byte, r.ecN)
		for i := range chunks {
			chunks[i] = make([]byte, r.chunkSize)
		}
		finished := false
		//outBools:要输出的若干块idx
		outBools := make([]bool, r.ecN)
		for i := range outBools {
			outBools[i] = false
		}
		for i := range outBlockIdx {
			outBools[outBlockIdx[i]] = true
		}
		constructIdx := make([]bool, r.ecN)
		for i := 0; i < r.ecN; i++ {
			constructIdx[i] = false
		}
		for i := 0; i < r.ecK; i++ {
			constructIdx[inBlockIdx[i]] = true
		}
		//nil Idx就是没有输入的块idx，要置成nil
		nilIdx := make([]int, r.ecP)
		ct := 0
		for i := 0; i < r.ecN; i++ {
			if !constructIdx[i] {
				nilIdx[ct] = i
				ct++
			}
		}

		for {
			//读块到buff
			for i := 0; i < r.ecK; i++ {
				_, err := input[i].Read(chunks[inBlockIdx[i]])
				if err != nil {
					finished = true
					break
				}
			}
			for i := 0; i < r.ecP; i++ {
				chunks[nilIdx[i]] = nil
			}
			if finished {
				break
			}
			//解码

			err := r.encoder.ReconstructSome(chunks, outBools)
			if err != nil {
				return
			}
			//输出到outWriter
			for i := range outBlockIdx {
				outWriter[i].Write(chunks[outBlockIdx[i]])
			}
		}
		for i := range input {
			input[i].Close()
		}
		for i := range outWriter {
			outWriter[i].Close()
		}
	}()
	return outReader, nil
}
