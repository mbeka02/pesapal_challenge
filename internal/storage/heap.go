package storage

import (
	"bytes"
	"encoding/binary"

	"github.com/mbeka02/pesapal_challenge/internal/types"
)

const (
	PAGE_HEADER_SIZE = 4
	SLOT_SIZE        = 4
)

type Heap struct {
	pager     *Pager
	startPage PageID
	numPages  uint32
}

func NewHeap(pager *Pager, start PageID) *Heap {
	return &Heap{pager: pager, startPage: start, numPages: 0}
}

func EncodeRow(row types.Row) []byte {
	buff := new(bytes.Buffer)
	for _, value := range row {
		switch t := value.(type) {
		case int:
			binary.Write(buff, binary.LittleEndian, int64(t))
		case float64:
			binary.Write(buff, binary.LittleEndian, t)
			// remember booleans are a byte
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

/*
Iterate() scans through all the pages
For each page, reads the number of cells from the header
For each cell, reads its slot to get offset and length
Extracts the data, decodes it using the schema, and calls the callback
Stops early if callback returns false
*/
func (h *Heap) Iterate(schema []types.Column, cb func(types.Row) bool) {
	// numPages := h.pager.NextPageID()
	for i := uint32(0); i < h.numPages; i++ {
		pageID := h.startPage + PageID(i)
		page, err := h.pager.ReadPage(pageID)
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

/*
My slotted page implementation
+----------------+
| Header (4B)    |  <- Fixed size header
+----------------+
| Slot Array     |  <- Grows downward (4B per slot)
+----------------+
|                |
| Free Space     |  <- Shrinks as data is added
|                |
+----------------+
| Data Cells     |  <- Grows upward from end of page
+----------------+
*/
func (h *Heap) Insert(data []byte) {
	// try and insert in the last page first , it might have space
	// nextPageID := h.pager.NextPageID()
	// if nextPageID > 0 {
	// 	lastPageID := nextPageID - 1
	// 	page, err := h.pager.ReadPage(lastPageID)
	// 	if err == nil {
	// 		if h.insertIntoPage(page, data) {
	// 			h.pager.WritePage(lastPageID, page)
	// 			return
	// 		}
	// 	} else {
	// 		fmt.Printf("Error reading last page: %v\n", err)
	// 	}
	// }
	if h.numPages > 0 {
		lastPage := h.startPage + PageID(h.numPages-1)
		page, err := h.pager.ReadPage(lastPage)
		if err == nil && h.insertIntoPage(page, data) {
			h.pager.WritePage(lastPage, page)
			return
		}
	}
	// allocate new page
	newPageID := h.startPage + PageID(h.numPages)
	page := make([]byte, PAGE_SIZE)
	h.initializePage(page)
	// just panic if the row can't fit
	if !h.insertIntoPage(page, data) {
		panic("Row too large for empty page")
	}
	h.pager.WritePage(newPageID, page)
	h.numPages++
}

/*
initializePage() creates a new page with the header
The header has a fixed size of 4 bytes
Bytes 0-2: NumCells (uint16) - number of records stored
Bytes 2-4: DataStart (uint16) - offset where data region begins (grows backward from PAGE_SIZE)
*/
func (h *Heap) initializePage(page []byte) {
	// Header:
	// 0-2: NumCells (uint16) = 0
	// 2-4: DataStart (uint16) = PAGE_SIZE
	binary.LittleEndian.PutUint16(page[0:2], 0)
	binary.LittleEndian.PutUint16(page[2:4], uint16(PAGE_SIZE))
}

func (h *Heap) insertIntoPage(page []byte, data []byte) bool {
	// gets how many records exist and where the data region currently starts.
	numCells := binary.LittleEndian.Uint16(page[0:2])
	dataStart := binary.LittleEndian.Uint16(page[2:4])

	// headerEnd is where the slot array ends (header + all existing slots). The free space is the gap between the end of the slot array and the start of the data region.
	headerEnd := PAGE_HEADER_SIZE + numCells*SLOT_SIZE
	freeSpace := int(dataStart) - int(headerEnd)

	needed := int(SLOT_SIZE) + len(data)

	if freeSpace < needed {
		return false
	}

	// update DataStart
	newDataStart := dataStart - uint16(len(data))

	// write Data
	copy(page[newDataStart:], data)

	// write Slot
	// slot: Offset (uint16), Size (uint16)
	slotOffset := headerEnd
	binary.LittleEndian.PutUint16(page[slotOffset:slotOffset+2], newDataStart)
	binary.LittleEndian.PutUint16(page[slotOffset+2:slotOffset+4], uint16(len(data)))

	// update Header
	binary.LittleEndian.PutUint16(page[0:2], numCells+1)
	binary.LittleEndian.PutUint16(page[2:4], newDataStart)

	return true
}
