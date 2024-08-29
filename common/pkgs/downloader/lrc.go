package downloader

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/math2"
)

func (iter *DownloadObjectIterator) downloadLRCObject(req downloadReqeust2, red *cdssdk.LRCRedundancy) (io.ReadCloser, error) {
	allNodes, err := iter.sortDownloadNodes(req)
	if err != nil {
		return nil, err
	}

	var blocks []downloadBlock
	selectedBlkIdx := make(map[int]bool)
	for _, node := range allNodes {
		for _, b := range node.Blocks {
			if b.Index >= red.M() || selectedBlkIdx[b.Index] {
				continue
			}
			blocks = append(blocks, downloadBlock{
				Node:  node.Node,
				Block: b,
			})
			selectedBlkIdx[b.Index] = true
		}
	}
	if len(blocks) < red.K {
		return nil, fmt.Errorf("not enough blocks to download lrc object")
	}

	var logStrs []any = []any{"downloading lrc object from blocks: "}
	for i, b := range blocks {
		if i > 0 {
			logStrs = append(logStrs, ", ")
		}
		logStrs = append(logStrs, fmt.Sprintf("%v@%v(%v)", b.Block.Index, b.Node.Name, b.Node.NodeID))
	}
	logger.Debug(logStrs...)

	pr, pw := io.Pipe()
	go func() {
		readPos := req.Raw.Offset
		totalReadLen := req.Detail.Object.Size - req.Raw.Offset
		if req.Raw.Length >= 0 {
			totalReadLen = math2.Min(req.Raw.Length, totalReadLen)
		}

		firstStripIndex := readPos / int64(red.K) / int64(red.ChunkSize)
		stripIter := NewLRCStripIterator(req.Detail.Object, blocks, red, firstStripIndex, iter.downloader.strips, iter.downloader.cfg.ECStripPrefetchCount)
		defer stripIter.Close()

		for totalReadLen > 0 {
			strip, err := stripIter.MoveNext()
			if err == iterator.ErrNoMoreItem {
				pw.CloseWithError(io.ErrUnexpectedEOF)
				return
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}

			readRelativePos := readPos - strip.Position
			nextStripPos := strip.Position + int64(red.K)*int64(red.ChunkSize)
			curReadLen := math2.Min(totalReadLen, nextStripPos-readPos)

			err = io2.WriteAll(pw, strip.Data[readRelativePos:readRelativePos+curReadLen])
			if err != nil {
				pw.CloseWithError(err)
				return
			}

			totalReadLen -= curReadLen
			readPos += curReadLen
		}
		pw.Close()
	}()

	return pr, nil
}
