package main

import (
	"fmt"
	"log"
	"os"

	"github.com/mbeka02/pesapal_challenge/internal/db"
	"github.com/mbeka02/pesapal_challenge/internal/types"
)

func main() {
	// start fresh to avoid layout conflicts
	os.Remove("test.db")

	database, err := db.OpenDB("test.db")
	if err != nil {
		log.Fatalf("unable to open the database:%v", err)
	}
	fmt.Println("Inserting data...")
	database.CreateTable("users", []types.Column{
		{Name: "id", Type: types.INT},
		{Name: "name", Type: types.TEXT},
		{Name: "is_admin", Type: types.BOOLEAN},
		{Name: "score", Type: types.FLOAT},
	})
	database.CreateTable("products", []types.Column{
		{Name: "id", Type: types.INT},
		{Name: "name", Type: types.TEXT},
		{Name: "price", Type: types.FLOAT},
	})
	users := database.Tables["users"]
	products := database.Tables["products"]
	users.Insert(types.Row{1, "Trevor", true, 95.5})
	users.Insert(types.Row{2, "Jane", false, 88.0})
	users.Insert(types.Row{3, "Bob", false, 72.3})
	products.Insert(types.Row{1, "Jik", 100.0})
	fmt.Println("Scanning data...")
	users.Scan(func(row types.Row) bool {
		fmt.Printf("ID: %v, Name: %v, Admin: %v, Score: %v\n", row[0], row[1], row[2], row[3])
		return true
	})
	products.Scan(func(row types.Row) bool {
		fmt.Printf("ID: %v, Name: %v, Price: %v\n", row[0], row[1], row[2])
		return true
	})
}
