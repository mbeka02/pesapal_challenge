package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/mbeka02/pesapal_challenge/internal/types"
)

const (
	PAGE_HEADER_SIZE = 4
	SLOT_SIZE        = 4
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
		case float64:
			binary.Write(buff, binary.LittleEndian, t)
		case bool:
			var b int8 = 0
			if t {
				b = 1
			}
			binary.Write(buff, binary.LittleEndian, b)
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
		case types.FLOAT:
			var v float64
			binary.Read(buff, binary.LittleEndian, &v)
			row = append(row, v)
		case types.BOOLEAN:
			var v int8
			binary.Read(buff, binary.LittleEndian, &v)
			row = append(row, v == 1)
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

func (h *Heap) Iterate(schema []types.Column, cb func(types.Row) bool) {
	numPages := h.pager.NextPageID()
	for i := PageID(0); i < numPages; i++ {
		page, err := h.pager.ReadPage(i)
		if err != nil {
			continue
		}

		numCells := binary.LittleEndian.Uint16(page[0:2])
		for cellIdx := uint16(0); cellIdx < numCells; cellIdx++ {
			slotOffset := PAGE_HEADER_SIZE + cellIdx*SLOT_SIZE

			recordOffset := binary.LittleEndian.Uint16(page[slotOffset : slotOffset+2])
			recordLen := binary.LittleEndian.Uint16(page[slotOffset+2 : slotOffset+4])

			recordData := page[recordOffset : recordOffset+recordLen]

			row := DecodeRow(recordData, schema)
			if !cb(row) {
				return
			}
		}
	}
}

func (h *Heap) Insert(data []byte) {
	// Try the last page first
	nextPageID := h.pager.NextPageID()
	if nextPageID > 0 {
		lastPageID := nextPageID - 1
		page, err := h.pager.ReadPage(lastPageID)
		if err == nil {
			if h.insertIntoPage(page, data) {
				h.pager.WritePage(lastPageID, page)
				return
			}
		} else {
			fmt.Printf("Error reading last page: %v\n", err)
		}
	}

	// Allocate new page
	pageID := nextPageID
	page := make([]byte, PAGE_SIZE)
	h.initPage(page)
	if !h.insertIntoPage(page, data) {
		panic("Row too large for empty page")
	}
	h.pager.WritePage(pageID, page)
}

func (h *Heap) initPage(page []byte) {
	// Header:
	// 0-2: NumCells (uint16) = 0
	// 2-4: DataStart (uint16) = PAGE_SIZE
	binary.LittleEndian.PutUint16(page[0:2], 0)
	binary.LittleEndian.PutUint16(page[2:4], uint16(PAGE_SIZE))
}

func (h *Heap) insertIntoPage(page []byte, data []byte) bool {
	numCells := binary.LittleEndian.Uint16(page[0:2])
	dataStart := binary.LittleEndian.Uint16(page[2:4])

	// Calculate free space
	// Free Space is between the end of the last slot and the start of data.
	headerEnd := PAGE_HEADER_SIZE + numCells*SLOT_SIZE
	freeSpace := int(dataStart) - int(headerEnd)

	needed := int(SLOT_SIZE) + len(data)

	if freeSpace < needed {
		return false
	}

	// Update DataStart
	newDataStart := dataStart - uint16(len(data))

	// Write Data
	copy(page[newDataStart:], data)

	// Write Slot
	// Slot: Offset (uint16), Size (uint16)
	slotOffset := headerEnd
	binary.LittleEndian.PutUint16(page[slotOffset:slotOffset+2], newDataStart)
	binary.LittleEndian.PutUint16(page[slotOffset+2:slotOffset+4], uint16(len(data)))

	// Update Header
	binary.LittleEndian.PutUint16(page[0:2], numCells+1)
	binary.LittleEndian.PutUint16(page[2:4], newDataStart)

	return true
}
