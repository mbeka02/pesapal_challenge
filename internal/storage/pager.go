package storage

import (
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

func (p *Pager) ReadPage(id PageID) ([]byte, error) {
	buff := make([]byte, PAGE_SIZE)
	offset := int64(id) * PAGE_SIZE

	_, err := p.file.ReadAt(buff, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buff, nil
}

func (p *Pager) WritePage(id PageID, Page []byte) (int, error) {
	// if len(data) != PAGE_SIZE {
	// 	return 0, fmt.Errorf("Invalid page size")
	// }
	offset := int64(id) * PAGE_SIZE
	return p.file.WriteAt(Page, offset)
}

// PAGES ARE ZERO-INDEXED
func (p *Pager) NextPageID() PageID {
	stat, err := p.file.Stat()
	if err != nil {
		panic(err)
	}
	return PageID(stat.Size() / PAGE_SIZE)
}
