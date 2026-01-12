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
