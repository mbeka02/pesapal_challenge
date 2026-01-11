package storage

import (
	"fmt"
	"io"
	"os"
)

type Pager struct {
	file *os.File
}

func NewPager(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &Pager{file: f}, nil
}

func (p *Pager) Read(id PageID) ([]byte, error) {
	buff := make([]byte, PAGE_SIZE)
	offset := int64(id) * PAGE_SIZE

	_, err := p.file.ReadAt(buff, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buff, nil
}

func (p *Pager) Write(id PageID, data []byte) (int, error) {
	if len(data) != PAGE_SIZE {
		return 0, fmt.Errorf("Invalid page size")
	}
	// write the data after the offset
	offset := int64(id) * PAGE_SIZE
	return p.file.WriteAt(data, offset)
}
