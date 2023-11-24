package ec

import (
	"bytes"
	"io"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_EncodeReconstruct(t *testing.T) {
	Convey("编码后使用校验块重建数据", t, func() {
		rs, err := NewRs(2, 3, 5)
		So(err, ShouldBeNil)

		outputs := rs.EncodeAll([]io.Reader{
			bytes.NewReader([]byte{1, 2, 3, 4, 5}),
			bytes.NewReader([]byte{6, 7, 8, 9, 10}),
		})

		var outputData = [][]byte{
			make([]byte, 5),
			make([]byte, 5),
			make([]byte, 5),
		}

		{ // 编码所有块
			errs := make([]error, 3)

			wg := sync.WaitGroup{}
			for i := range outputs {
				idx := i

				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := io.ReadFull(outputs[idx], outputData[idx])
					errs[idx] = err
				}()
			}

			wg.Wait()

			for _, e := range errs {
				if e != io.EOF {
					So(e, ShouldBeNil)
				}
			}

			So(outputData[0], ShouldResemble, []byte{1, 2, 3, 4, 5})
			So(outputData[1], ShouldResemble, []byte{6, 7, 8, 9, 10})
		}

		{ // 重建所有数据块
			recOutputs := rs.ReconstructData([]io.Reader{
				bytes.NewBuffer(outputData[1]),
				bytes.NewBuffer(outputData[2]),
			}, []int{1, 2})

			recOutputData := [][]byte{
				make([]byte, 5),
				make([]byte, 5),
			}
			errs := make([]error, 2)

			wg := sync.WaitGroup{}
			for i := range recOutputs {
				idx := i

				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := io.ReadFull(recOutputs[idx], recOutputData[idx])
					errs[idx] = err
				}()
			}

			wg.Wait()

			for _, e := range errs {
				if e != io.EOF {
					So(e, ShouldBeNil)
				}
			}

			So(recOutputData[0], ShouldResemble, []byte{1, 2, 3, 4, 5})
			So(recOutputData[1], ShouldResemble, []byte{6, 7, 8, 9, 10})
		}

		{ // 重建指定的数据块
			recOutputs := rs.ReconstructSome([]io.Reader{
				bytes.NewBuffer(outputData[1]),
				bytes.NewBuffer(outputData[2]),
			}, []int{1, 2}, []int{0, 1})

			recOutputData := [][]byte{
				make([]byte, 5),
				make([]byte, 5),
			}
			errs := make([]error, 2)

			wg := sync.WaitGroup{}
			for i := range recOutputs {
				idx := i

				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := io.ReadFull(recOutputs[idx], recOutputData[idx])
					errs[idx] = err
				}()
			}

			wg.Wait()

			for _, e := range errs {
				if e != io.EOF {
					So(e, ShouldBeNil)
				}
			}

			So(recOutputData[0], ShouldResemble, []byte{1, 2, 3, 4, 5})
			So(recOutputData[1], ShouldResemble, []byte{6, 7, 8, 9, 10})
		}

		{ // 重建指定的数据块
			recOutputs := rs.ReconstructSome([]io.Reader{
				bytes.NewBuffer(outputData[1]),
				bytes.NewBuffer(outputData[2]),
			}, []int{1, 2}, []int{0})

			recOutputData := [][]byte{
				make([]byte, 5),
			}
			errs := make([]error, 2)

			wg := sync.WaitGroup{}
			for i := range recOutputs {
				idx := i

				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := io.ReadFull(recOutputs[idx], recOutputData[idx])
					errs[idx] = err
				}()
			}

			wg.Wait()

			for _, e := range errs {
				if e != io.EOF {
					So(e, ShouldBeNil)
				}
			}

			So(recOutputData[0], ShouldResemble, []byte{1, 2, 3, 4, 5})
		}

		{ // 重建指定的数据块
			recOutputs := rs.ReconstructSome([]io.Reader{
				bytes.NewBuffer(outputData[1]),
				bytes.NewBuffer(outputData[2]),
			}, []int{1, 2}, []int{1})

			recOutputData := [][]byte{
				make([]byte, 5),
			}
			errs := make([]error, 2)

			wg := sync.WaitGroup{}
			for i := range recOutputs {
				idx := i

				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := io.ReadFull(recOutputs[idx], recOutputData[idx])
					errs[idx] = err
				}()
			}

			wg.Wait()

			for _, e := range errs {
				if e != io.EOF {
					So(e, ShouldBeNil)
				}
			}

			So(recOutputData[0], ShouldResemble, []byte{6, 7, 8, 9, 10})
		}

		{ // 单独产生校验块
			encOutputs := rs.Encode([]io.Reader{
				bytes.NewBuffer(outputData[0]),
				bytes.NewBuffer(outputData[1]),
			})

			encOutputData := [][]byte{
				make([]byte, 5),
			}
			errs := make([]error, 2)

			wg := sync.WaitGroup{}
			for i := range encOutputs {
				idx := i

				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := io.ReadFull(encOutputs[idx], encOutputData[idx])
					errs[idx] = err
				}()
			}

			wg.Wait()

			for _, e := range errs {
				if e != io.EOF {
					So(e, ShouldBeNil)
				}
			}

			So(encOutputData[0], ShouldResemble, outputData[2])
		}

		{ // 使用ReconstructAny单独重建校验块
			encOutputs := rs.ReconstructAny([]io.Reader{
				bytes.NewBuffer(outputData[0]),
				bytes.NewBuffer(outputData[1]),
			}, []int{0, 1}, []int{2})

			encOutputData := [][]byte{
				make([]byte, 5),
			}
			errs := make([]error, 2)

			wg := sync.WaitGroup{}
			for i := range encOutputs {
				idx := i

				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := io.ReadFull(encOutputs[idx], encOutputData[idx])
					errs[idx] = err
				}()
			}

			wg.Wait()

			for _, e := range errs {
				if e != io.EOF {
					So(e, ShouldBeNil)
				}
			}

			So(encOutputData[0], ShouldResemble, outputData[2])
		}
	})
}
