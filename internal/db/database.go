package db

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/mbeka02/pesapal_challenge/internal/storage"
	"github.com/mbeka02/pesapal_challenge/internal/types"
)

type DB struct {
	Tables map[string]*Table
	Pager  *storage.Pager
}

func insertCatalogEntry(pager *storage.Pager, entry CatalogEntry) error {
	page, err := pager.ReadPage(0)
	if err != nil {
		return err
	}

	data := EncodeCatalogEntry(entry)

	heap := storage.NewHeap(pager, 0)
	if !heap.InsertRaw(page, data) {
		return fmt.Errorf("catalog page full")
	}
	_, err = pager.WritePage(0, page)
	return err
}

// updateCatalogEntry updates an existing catalog entry in-place
func updateCatalogEntry(pager *storage.Pager, tableName string, numPages uint32) error {
	page, err := pager.ReadPage(0)
	if err != nil {
		return err
	}

	numEntries := binary.LittleEndian.Uint16(page[0:2])

	// find the entry to update
	for i := uint16(0); i < numEntries; i++ {
		slotOffset := storage.PAGE_HEADER_SIZE + i*storage.SLOT_SIZE
		recordOffset := binary.LittleEndian.Uint16(page[slotOffset : slotOffset+2])
		recordLen := binary.LittleEndian.Uint16(page[slotOffset+2 : slotOffset+4])
		record := page[recordOffset : recordOffset+recordLen]

		entry := DecodeCatalogEntry(record)

		if entry.Name == tableName {

			// update the entry
			entry.NumPages = numPages
			newRecord := EncodeCatalogEntry(entry)

			// verify sizes match (they should since we're only updating NumPages, a u32)
			if len(newRecord) != len(record) {
				return fmt.Errorf("catalog entry size mismatch: old=%d new=%d", len(record), len(newRecord))
			}

			// update in place
			copy(page[recordOffset:recordOffset+recordLen], newRecord)
			_, err = pager.WritePage(0, page)

			return err
		}
	}

	return fmt.Errorf("table %s not found in catalog", tableName)
}

func (db *DB) CreateTable(name string, schema []types.Column) error {
	if _, exists := db.Tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	// load fresh catalog data
	entries, err := LoadCatalog(db.Pager)
	if err != nil {
		return err
	}
	// find next free page
	var nextPage uint64 = 1
	for _, e := range entries {
		// each table needs at least its start page, even if NumPages=0
		// if NumPages > 0, it occupies [StartPage, StartPage+NumPages)
		// if NumPages = 0, it still reserves StartPage for future use
		end := e.StartPage + uint64(e.NumPages)
		if e.NumPages == 0 {
			// reserve at least one page for tables with no data yet
			end = e.StartPage + 1
		}
		if end > nextPage {
			nextPage = end
		}
	}

	entry := CatalogEntry{
		Name:      name,
		StartPage: nextPage,
		NumPages:  0,
		Schema:    schema,
	}

	if err := insertCatalogEntry(db.Pager, entry); err != nil {
		return err
	}

	heap := storage.NewHeap(db.Pager, storage.PageID(entry.StartPage))

	// set callback to update catalog when heap grows
	heap.SetGrowthCallback(func(newNumPages uint32) {
		if err := updateCatalogEntry(db.Pager, name, newNumPages); err != nil {
			log.Printf("ERROR updating catalog: %v", err)
		}
	})

	db.Tables[name] = &Table{
		Name:   name,
		Schema: schema,
		Heap:   heap,
	}

	return nil
}

func OpenDB(path string) (*DB, error) {
	pager, err := storage.NewPager(path)
	if err != nil {
		return nil, err
	}

	db := &DB{
		Tables: make(map[string]*Table),
		Pager:  pager,
	}

	entries, err := LoadCatalog(pager)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		heap := storage.NewHeap(pager, storage.PageID(e.StartPage))
		heap.SetNumPages(e.NumPages)

		// capture table name for closure
		tableName := e.Name
		heap.SetGrowthCallback(func(newNumPages uint32) {
			if err := updateCatalogEntry(db.Pager, tableName, newNumPages); err != nil {
				log.Printf("ERROR updating catalog: %v", err)
			}
		})

		db.Tables[e.Name] = &Table{
			Name:   e.Name,
			Schema: e.Schema,
			Heap:   heap,
		}
	}

	return db, nil
}
