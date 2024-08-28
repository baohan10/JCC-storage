package downloader

import (
	"context"
	"io"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc/parser"
)

type LRCStripIterator struct {
	object              cdssdk.Object
	blocks              []downloadBlock
	red                 *cdssdk.LRCRedundancy
	curStripIndex       int64
	cache               *StripCache
	dataChan            chan dataChanEntry
	downloadingDone     chan any
	downloadingDoneOnce sync.Once
	inited              bool
}

func NewLRCStripIterator(object cdssdk.Object, blocks []downloadBlock, red *cdssdk.LRCRedundancy, beginStripIndex int64, cache *StripCache, maxPrefetch int) *LRCStripIterator {
	if maxPrefetch <= 0 {
		maxPrefetch = 1
	}

	iter := &LRCStripIterator{
		object:          object,
		blocks:          blocks,
		red:             red,
		curStripIndex:   beginStripIndex,
		cache:           cache,
		dataChan:        make(chan dataChanEntry, maxPrefetch-1),
		downloadingDone: make(chan any),
	}

	return iter
}

func (s *LRCStripIterator) MoveNext() (Strip, error) {
	if !s.inited {
		go s.downloading()
		s.inited = true
	}

	// 先尝试获取一下，用于判断本次获取是否发生了等待
	select {
	case entry, ok := <-s.dataChan:
		if !ok || entry.Error == io.EOF {
			return Strip{}, iterator.ErrNoMoreItem
		}

		if entry.Error != nil {
			return Strip{}, entry.Error
		}

		s.curStripIndex++
		return Strip{Data: entry.Data, Position: entry.Position}, nil

	default:
		logger.Debugf("waitting for ec strip %v of object %v", s.curStripIndex, s.object.ObjectID)
	}

	// 发生了等待
	select {
	case entry, ok := <-s.dataChan:
		if !ok || entry.Error == io.EOF {
			return Strip{}, iterator.ErrNoMoreItem
		}

		if entry.Error != nil {
			return Strip{}, entry.Error
		}

		s.curStripIndex++
		return Strip{Data: entry.Data, Position: entry.Position}, nil

	case <-s.downloadingDone:
		return Strip{}, iterator.ErrNoMoreItem
	}
}

func (s *LRCStripIterator) Close() {
	s.downloadingDoneOnce.Do(func() {
		close(s.downloadingDone)
	})
}

func (s *LRCStripIterator) downloading() {
	var froms []ioswitchlrc.From
	for _, b := range s.blocks {
		froms = append(froms, ioswitchlrc.NewFromNode(b.Block.FileHash, &b.Node, b.Block.Index))
	}

	toExec, hd := ioswitchlrc.NewToDriverWithRange(-1, exec.Range{
		Offset: s.curStripIndex * int64(s.red.ChunkSize*s.red.K),
	})

	plans := exec.NewPlanBuilder()
	err := parser.ReconstructAny(froms, []ioswitchlrc.To{toExec}, plans)
	if err != nil {
		s.sendToDataChan(dataChanEntry{Error: err})
		return
	}
	exec := plans.Execute()

	ctx, cancel := context.WithCancel(context.Background())
	go exec.Wait(ctx)
	defer cancel()

	str, err := exec.BeginRead(hd)
	if err != nil {
		s.sendToDataChan(dataChanEntry{Error: err})
		return
	}

	curStripIndex := s.curStripIndex
loop:
	for {
		stripBytesPos := curStripIndex * int64(s.red.K) * int64(s.red.ChunkSize)
		if stripBytesPos >= s.object.Size {
			s.sendToDataChan(dataChanEntry{Error: io.EOF})
			break
		}

		stripKey := ECStripKey{
			ObjectID:   s.object.ObjectID,
			StripIndex: curStripIndex,
		}

		item, ok := s.cache.Get(stripKey)
		if ok {
			if item.ObjectFileHash == s.object.FileHash {
				if !s.sendToDataChan(dataChanEntry{Data: item.Data, Position: stripBytesPos}) {
					break loop
				}
				curStripIndex++
				continue

			} else {
				// 如果Object的Hash和Cache的Hash不一致，说明Cache是无效的，需要重新下载
				s.cache.Remove(stripKey)
			}
		}

		dataBuf := make([]byte, int64(s.red.K*s.red.ChunkSize))
		n, err := io.ReadFull(str, dataBuf)
		if err == io.ErrUnexpectedEOF {
			s.cache.Add(stripKey, ObjectECStrip{
				Data:           dataBuf,
				ObjectFileHash: s.object.FileHash,
			})

			s.sendToDataChan(dataChanEntry{Data: dataBuf[:n], Position: stripBytesPos})
			s.sendToDataChan(dataChanEntry{Error: io.EOF})
			break loop
		}
		if err != nil {
			s.sendToDataChan(dataChanEntry{Error: err})
			break loop
		}

		s.cache.Add(stripKey, ObjectECStrip{
			Data:           dataBuf,
			ObjectFileHash: s.object.FileHash,
		})

		if !s.sendToDataChan(dataChanEntry{Data: dataBuf, Position: stripBytesPos}) {
			break loop
		}

		curStripIndex++
	}

	close(s.dataChan)
}

func (s *LRCStripIterator) sendToDataChan(entry dataChanEntry) bool {
	select {
	case s.dataChan <- entry:
		return true
	case <-s.downloadingDone:
		return false
	}
}
