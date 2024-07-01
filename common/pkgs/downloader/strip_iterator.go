package downloader

import (
	"io"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sync2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
)

type downloadBlock struct {
	Node  cdssdk.Node
	Block stgmod.ObjectBlock
}

type Strip struct {
	Data     []byte
	Position int64
}

type StripIterator struct {
	object              cdssdk.Object
	blocks              []downloadBlock
	red                 *cdssdk.ECRedundancy
	curStripPos         int64
	cache               *StripCache
	dataChan            chan dataChanEntry
	downloadingDone     chan any
	downloadingDoneOnce sync.Once
	inited              bool
}

type dataChanEntry struct {
	Data     []byte
	Position int64 // 条带在文件中的位置。字节为单位
	Error    error
}

func NewStripIterator(object cdssdk.Object, blocks []downloadBlock, red *cdssdk.ECRedundancy, beginStripPos int64, cache *StripCache, maxPrefetch int) *StripIterator {
	if maxPrefetch <= 0 {
		maxPrefetch = 1
	}

	iter := &StripIterator{
		object:          object,
		blocks:          blocks,
		red:             red,
		curStripPos:     beginStripPos,
		cache:           cache,
		dataChan:        make(chan dataChanEntry, maxPrefetch-1),
		downloadingDone: make(chan any),
	}

	return iter
}

func (s *StripIterator) MoveNext() (Strip, error) {
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

		s.curStripPos++
		return Strip{Data: entry.Data, Position: entry.Position}, nil

	default:
		logger.Debugf("waitting for ec strip %v for object %v", s.curStripPos, s.object.ObjectID)
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

		s.curStripPos++
		return Strip{Data: entry.Data, Position: entry.Position}, nil

	case <-s.downloadingDone:
		return Strip{}, iterator.ErrNoMoreItem
	}
}

func (s *StripIterator) Close() {
	s.downloadingDoneOnce.Do(func() {
		close(s.downloadingDone)
	})
}

func (s *StripIterator) downloading() {
	rs, err := ec.NewRs(s.red.K, s.red.N)
	if err != nil {
		s.sendToDataChan(dataChanEntry{Error: err})
		return
	}

	var blockStrs []*IPFSReader
	for _, b := range s.blocks {
		blockStrs = append(blockStrs, NewIPFSReader(b.Node, b.Block.FileHash))
	}

	curStripPos := s.curStripPos
loop:
	for {
		stripBytesPos := curStripPos * int64(s.red.ChunkSize)
		if stripBytesPos >= s.object.Size {
			s.sendToDataChan(dataChanEntry{Error: io.EOF})
			break
		}

		stripKey := ECStripKey{
			ObjectID:      s.object.ObjectID,
			StripPosition: curStripPos,
		}

		item, ok := s.cache.Get(stripKey)
		if ok {
			if item.ObjectFileHash == s.object.FileHash {
				if !s.sendToDataChan(dataChanEntry{Data: item.Data, Position: stripBytesPos}) {
					break loop
				}
				curStripPos++
				continue

			} else {
				// 如果Object的Hash和Cache的Hash不一致，说明Cache是无效的，需要重新下载
				s.cache.Remove(stripKey)
			}
		}

		for _, str := range blockStrs {
			_, err := str.Seek(stripBytesPos*int64(s.red.ChunkSize), io.SeekStart)
			if err != nil {
				s.sendToDataChan(dataChanEntry{Error: err})
				break loop
			}
		}

		dataBuf := make([]byte, int64(s.red.K*s.red.ChunkSize))
		blockArrs := make([][]byte, s.red.N)
		for i := 0; i < s.red.K; i++ {
			// 放入的slice长度为0，但容量为ChunkSize，EC库发现长度为0的块后才会认为是待恢复块
			blockArrs[i] = dataBuf[i*s.red.ChunkSize : i*s.red.ChunkSize]
		}
		for _, b := range s.blocks {
			// 用于恢复的块则要将其长度变回ChunkSize，用于后续读取块数据
			if b.Block.Index < s.red.K {
				// 此处扩容不会导致slice指向一个新内存
				blockArrs[b.Block.Index] = blockArrs[b.Block.Index][0:s.red.ChunkSize]
			} else {
				blockArrs[b.Block.Index] = make([]byte, s.red.ChunkSize)
			}
		}

		err := sync2.ParallelDo(s.blocks, func(b downloadBlock, idx int) error {
			_, err := io.ReadFull(blockStrs[idx], blockArrs[b.Block.Index])
			return err
		})
		if err != nil {
			s.sendToDataChan(dataChanEntry{Error: err})
			break loop
		}

		err = rs.ReconstructData(blockArrs)
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
	}

	for _, str := range blockStrs {
		str.Close()
	}

	close(s.dataChan)
}

func (s *StripIterator) sendToDataChan(entry dataChanEntry) bool {
	select {
	case s.dataChan <- entry:
		return true
	case <-s.downloadingDone:
		return false
	}
}
