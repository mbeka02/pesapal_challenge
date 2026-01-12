package db

import (
	"github.com/mbeka02/pesapal_challenge/internal/storage"
	"github.com/mbeka02/pesapal_challenge/internal/types"
)

type Table struct {
	Name   string
	Schema []types.Column
	Heap   *storage.Heap
}

func (t *Table) Insert(row types.Row) {
	data := storage.EncodeRow(row)
	t.Heap.Insert(data)
}

func (t *Table) Scan(cb func(types.Row) bool) {
	t.Heap.Iterate(t.Schema, cb)
}