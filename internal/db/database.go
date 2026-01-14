package db

import (
	"fmt"

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

	heap := storage.NewHeap(pager, 0) // temporary helper
	if !heap.InsertRaw(page, data) {
		return fmt.Errorf("catalog page full")
	}
	_, err = pager.WritePage(0, page)
	return err
}

func (db *DB) CreateTable(name string, schema []types.Column) error {
	// Prevent duplicates
	if _, exists := db.Tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	entries, err := LoadCatalog(db.Pager)
	if err != nil {
		return err
	}

	// Find next free page (simple linear allocator)
	var nextPage uint64 = 1 // page 0 is catalog
	for _, e := range entries {
		end := e.StartPage + uint64(e.NumPages)
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

	// Append to catalog page
	if err := insertCatalogEntry(db.Pager, entry); err != nil {
		return err
	}

	heap := storage.NewHeap(db.Pager, storage.PageID(entry.StartPage))

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
		heap := storage.NewHeap(
			pager,
			storage.PageID(e.StartPage),
		)
		heap.SetNumPages(e.NumPages)

		db.Tables[e.Name] = &Table{
			Name:   e.Name,
			Schema: e.Schema,
			Heap:   heap,
		}
	}

	return db, nil
}
