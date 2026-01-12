package storage

import (
	"bytes"
	"encoding/binary"

	"github.com/mbeka02/pesapal_challenge/internal/types"
)

type Heap struct {
	pager *Pager
}

func NewHeap(pager *Pager) *Heap {
	return &Heap{pager}
}

func EncodeRow(row types.Row) []byte {
	buff := new(bytes.Buffer)
	for _, value := range row {
		switch t := value.(type) {
		case int:
			binary.Write(buff, binary.LittleEndian, int64(t))
		case string:
			binary.Write(buff, binary.LittleEndian, int32(len(t)))
			buff.Write([]byte(t))
		default:
			panic("unknown type")
		}
	}
	return buff.Bytes()
}

func DecodeRow(data []byte, schema []types.Column) types.Row {
	buff := bytes.NewReader(data)
	row := make(types.Row, 0)
	for _, column := range schema {
		switch column.Type {
		case types.INT:
			var v int64
			binary.Read(buff, binary.LittleEndian, &v)
			row = append(row, int(v))
		case types.TEXT:
			var contentLength int32
			binary.Read(buff, binary.LittleEndian, &contentLength)
			b := make([]byte, contentLength)
			buff.Read(b)
			row = append(row, string(b))
		}
	}
	return row
}

// I'm opting for a really simplified layout at the moment , one row per page
func (h *Heap) Insert(data []byte) {
	pageID := h.pager.NextPageID()
	page := make([]byte, PAGE_SIZE)

	binary.LittleEndian.PutUint32(page[0:4], 1)
	binary.LittleEndian.PutUint32(page[4:8], uint32(len(data)))

	copy(page[8:], data)

	h.pager.WritePage(pageID, page)
}
