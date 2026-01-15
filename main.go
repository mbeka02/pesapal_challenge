package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/mbeka02/pesapal_challenge/internal/db"
	"github.com/mbeka02/pesapal_challenge/internal/executor"
	"github.com/mbeka02/pesapal_challenge/internal/parser"
)

func main() {
	os.Remove("test.db")
	database, err := db.OpenDB("test.db")
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}

	exec := executor.NewExecutor(database)

	fmt.Println("╔════════════════════════════════════════╗")
	fmt.Println("║   Simple Go RDBMS - SQL Interface     ║")
	fmt.Println("╚════════════════════════════════════════╝")
	fmt.Println("Type SQL commands (end with ;) or 'exit' to quit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	var inputBuffer strings.Builder

	for {
		if inputBuffer.Len() == 0 {
			fmt.Print("sql> ")
		} else {
			fmt.Print("  -> ")
		}

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())

		if line == "exit" || line == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		if line == "" {
			continue
		}

		// Accumulate input until we see a semicolon
		inputBuffer.WriteString(line)
		inputBuffer.WriteString(" ")

		if !strings.HasSuffix(line, ";") {
			continue
		}

		// handle a complete statement
		input := inputBuffer.String()
		inputBuffer.Reset()

		// Parse and execute
		sql, err := parser.Parse(input)
		if err != nil {
			fmt.Printf("Error: %v\n\n", err)
			continue
		}

		result, err := exec.Execute(sql)
		if err != nil {
			fmt.Printf("Error: %v\n\n", err)
			continue
		}

		fmt.Println(result)
		fmt.Println()
	}
}
