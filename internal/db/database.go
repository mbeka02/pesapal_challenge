package db

import "github.com/mbeka02/pesapal_challenge/internal/storage"

type DB struct {
	Tables map[string]*Table
	Pager  *storage.Pager
}

func OpenDB(path string) (*DB, error) {
	pager, err := storage.NewPager(path)
	if err != nil {
		return nil, err
	}
	return &DB{
		Tables: make(map[string]*Table),
		Pager:  pager,
	}, nil
}
