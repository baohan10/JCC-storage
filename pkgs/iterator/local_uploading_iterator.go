package iterator

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

type UploadingObjectIterator = Iterator[*IterUploadingObject]

type LocalUploadingIterator struct {
	pathRoot     string
	filePathes   []string
	currentIndex int
}

type IterUploadingObject struct {
	Path string
	Size int64
	File io.ReadCloser
}

func NewUploadingObjectIterator(pathRoot string, filePathes []string) *LocalUploadingIterator {
	return &LocalUploadingIterator{
		pathRoot:   filepath.ToSlash(pathRoot),
		filePathes: filePathes,
	}
}

func (i *LocalUploadingIterator) MoveNext() (*IterUploadingObject, error) {
	if i.currentIndex >= len(i.filePathes) {
		return nil, ErrNoMoreItem
	}

	item, err := i.doMove()
	i.currentIndex++
	return item, err
}

func (i *LocalUploadingIterator) doMove() (*IterUploadingObject, error) {
	path := i.filePathes[i.currentIndex]

	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &IterUploadingObject{
		Path: strings.TrimPrefix(filepath.ToSlash(path), i.pathRoot),
		Size: info.Size(),
		File: file,
	}, nil
}

func (i *LocalUploadingIterator) Close() {

}
