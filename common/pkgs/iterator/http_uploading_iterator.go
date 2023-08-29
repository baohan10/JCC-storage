package iterator

import (
	"mime/multipart"
)

type HTTPUploadingIterator struct {
	files        []*multipart.FileHeader
	currentIndex int
}

func NewHTTPObjectIterator(files []*multipart.FileHeader) *HTTPUploadingIterator {
	return &HTTPUploadingIterator{
		files: files,
	}
}

func (i *HTTPUploadingIterator) MoveNext() (*IterUploadingObject, error) {
	if i.currentIndex >= len(i.files) {
		return nil, ErrNoMoreItem
	}

	item, err := i.doMove()
	i.currentIndex++
	return item, err
}

func (i *HTTPUploadingIterator) doMove() (*IterUploadingObject, error) {
	fileInfo := i.files[i.currentIndex]

	file, err := fileInfo.Open()
	if err != nil {
		return nil, err
	}

	return &IterUploadingObject{
		Path: fileInfo.Filename,
		Size: fileInfo.Size,
		File: file,
	}, nil
}

func (i *HTTPUploadingIterator) Close() {

}
