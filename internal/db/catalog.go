package db

import (
	"bytes"
	"encoding/binary"

	"github.com/mbeka02/pesapal_challenge/internal/storage"
	"github.com/mbeka02/pesapal_challenge/internal/types"
)

type Catalog struct {
	pager *storage.Pager
}
type CatalogEntry struct {
	Name      string
	StartPage uint64
	NumPages  uint32
	Schema    []types.Column
}

/*Sets the Page Headers*/
func initializeCatalogPage(page []byte) {
	binary.LittleEndian.PutUint16(page[0:2], 0)                         // NumEntries
	binary.LittleEndian.PutUint16(page[2:4], uint16(storage.PAGE_SIZE)) // DataStart
}

func LoadCatalog(pager *storage.Pager) ([]CatalogEntry, error) {
	// if file is empty, initialize catalog page
	if pager.NextPageID() == 0 {
		page := make([]byte, storage.PAGE_SIZE)
		initializeCatalogPage(page)
		if _, err := pager.WritePage(0, page); err != nil {
			return nil, err
		}
		return nil, nil
	}

	page, err := pager.ReadPage(0)
	if err != nil {
		return nil, err
	}

	numEntries := binary.LittleEndian.Uint16(page[0:2])
	entries := make([]CatalogEntry, 0, numEntries)

	for i := uint16(0); i < numEntries; i++ {
		slotOffset := storage.PAGE_HEADER_SIZE + i*storage.SLOT_SIZE

		recordOffset := binary.LittleEndian.Uint16(page[slotOffset : slotOffset+2])
		recordLen := binary.LittleEndian.Uint16(page[slotOffset+2 : slotOffset+4])
		record := page[recordOffset : recordOffset+recordLen]
		entry := DecodeCatalogEntry(record)
		entries = append(entries, entry)
	}

	return entries, nil
}

// func (c *Catalog) CreateTable(entry CatalogEntry) error
// func (c *Catalog) ListTables() ([]CatalogEntry, error)

/*
Encoding format:
| nameLen (u16) | name bytes |
| startPage (u64) |
| numPages (u32) |
| numColumns (u16) |
| [ columnNameLen (u16) | columnName | columnType (u8) ] × N |
*/
func EncodeCatalogEntry(e CatalogEntry) []byte {
	buff := new(bytes.Buffer)
	// table name
	binary.Write(buff, binary.LittleEndian, uint16(len(e.Name)))
	buff.Write([]byte(e.Name))

	// heap info
	binary.Write(buff, binary.LittleEndian, e.StartPage)
	binary.Write(buff, binary.LittleEndian, e.NumPages)

	// schema
	binary.Write(buff, binary.LittleEndian, uint16(len(e.Schema)))
	for _, col := range e.Schema {
		binary.Write(buff, binary.LittleEndian, uint16(len(col.Name)))
		buff.Write([]byte(col.Name))
		binary.Write(buff, binary.LittleEndian, uint8(col.Type))
	}
	return buff.Bytes()
}

func DecodeCatalogEntry(data []byte) CatalogEntry {
	r := bytes.NewReader(data)

	var nameLen uint16
	binary.Read(r, binary.LittleEndian, &nameLen)
	name := make([]byte, nameLen)
	r.Read(name)

	var startPage uint64
	var numPages uint32
	binary.Read(r, binary.LittleEndian, &startPage)
	binary.Read(r, binary.LittleEndian, &numPages)

	var numCols uint16
	binary.Read(r, binary.LittleEndian, &numCols)

	schema := make([]types.Column, 0, numCols)
	for i := uint16(0); i < numCols; i++ {
		var colNameLen uint16
		binary.Read(r, binary.LittleEndian, &colNameLen)
		colName := make([]byte, colNameLen)
		r.Read(colName)

		var colType uint8
		binary.Read(r, binary.LittleEndian, &colType)

		schema = append(schema, types.Column{
			Name: string(colName),
			Type: types.DataType(colType),
		})
	}

	return CatalogEntry{
		Name:      string(name),
		StartPage: startPage,
		NumPages:  numPages,
		Schema:    schema,
	}
}
