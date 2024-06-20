package downloader

import (
	"sort"

	lru "github.com/hashicorp/golang-lru/v2"
	"gitlink.org.cn/cloudream/common/utils/lo2"
)

type LRUID int64

type cacheFile struct {
	Segments []*fileSegment
}

type fileSegment struct {
	LRUID  LRUID
	Offset int64
	Data   []byte
}

type lruEntry struct {
	FileHash string
	Offset   int64
}

type Cache struct {
	lru       *lru.Cache[LRUID, lruEntry]
	nextLRUID LRUID
	files     map[string]*cacheFile
}

func NewCache(size int) *Cache {
	c := &Cache{
		files: make(map[string]*cacheFile),
	}

	lru, _ := lru.NewWithEvict(size, c.onEvict)
	c.lru = lru

	return c
}

func (c *Cache) Put(fileHash string, offset int64, data []byte) {
	file, ok := c.files[fileHash]
	if !ok {
		file = &cacheFile{}
		c.files[fileHash] = file
	}

	idx := sort.Search(len(file.Segments), upperBound(file.Segments, offset))

	// 允许不同Segment之间有重叠，只在Offset相等时替换数据
	if idx < len(file.Segments) && file.Segments[idx].Offset == offset {
		file.Segments[idx].Data = data
		// Get一下更新LRU
		c.lru.Get(file.Segments[idx].LRUID)
	} else {
		file.Segments = lo2.Insert(file.Segments, idx, &fileSegment{
			LRUID:  c.nextLRUID,
			Offset: offset,
			Data:   data,
		})
		c.lru.Add(c.nextLRUID, lruEntry{
			FileHash: fileHash,
			Offset:   offset,
		})
		c.nextLRUID++
	}
}

func (c *Cache) Get(fileHash string, offset int64) []byte {
	file, ok := c.files[fileHash]
	if !ok {
		return nil
	}

	idx := sort.Search(len(file.Segments), upperBound(file.Segments, offset))
	if idx == 0 {
		return nil
	}
	seg := file.Segments[idx-1]
	// Get一下更新LRU
	c.lru.Get(seg.LRUID)

	return seg.Data[offset-seg.Offset:]
}

func (c *Cache) onEvict(key LRUID, value lruEntry) {
	// 不应该找不到文件或者分片
	file := c.files[value.FileHash]
	idx := sort.Search(len(file.Segments), upperBound(file.Segments, value.Offset))
	file.Segments = lo2.RemoveAt(file.Segments, idx)
	if len(file.Segments) == 0 {
		delete(c.files, value.FileHash)
	}
}

// 使用此函数会找到第一个大于等于 target 的索引，如果找不到，则返回 len(seg)
func upperBound(seg []*fileSegment, target int64) func(int) bool {
	return func(i int) bool {
		return seg[i].Offset >= target
	}
}
