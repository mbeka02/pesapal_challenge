package main

import (
	"log"

	"github.com/mbeka02/pesapal_challenge/internal/db"
	"github.com/mbeka02/pesapal_challenge/internal/storage"
	"github.com/mbeka02/pesapal_challenge/internal/types"
)

func main() {
	database, err := db.OpenDB("test.db")
	if err != nil {
		log.Fatalf("unable to open the database:%v", err)
	}
	heap := storage.NewHeap(database.Pager)

	users := &db.Table{
		Name: "users",
		Schema: []types.Column{
			{Name: "id", Type: types.INT},
			{Name: "name", Type: types.TEXT},
		},
		Heap: heap,
	}

	database.Tables["users"] = users

	encoded := storage.EncodeRow(types.Row{1, "Trevor"})
	heap.Insert(encoded)
}
