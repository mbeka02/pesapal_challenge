package executor

import (
	"fmt"
	"strings"

	"github.com/mbeka02/pesapal_challenge/internal/db"
	"github.com/mbeka02/pesapal_challenge/internal/parser"
	"github.com/mbeka02/pesapal_challenge/internal/types"
)

type Executor struct {
	db *db.DB
}

func NewExecutor(database *db.DB) *Executor {
	return &Executor{db: database}
}

func (e *Executor) Execute(sql *parser.SQL) (string, error) {
	if sql.CreateTable != nil {
		return e.executeCreateTable(sql.CreateTable)
	}
	if sql.Insert != nil {
		return e.executeInsert(sql.Insert)
	}
	if sql.Select != nil {
		return e.executeSelect(sql.Select)
	}
	return "", fmt.Errorf("unknown statement type")
}

func (e *Executor) executeCreateTable(stmt *parser.CreateTable) (string, error) {
	schema := make([]types.Column, len(stmt.Columns))
	for i, col := range stmt.Columns {
		dataType, err := parseDataType(col.Type)
		if err != nil {
			return "", err
		}
		schema[i] = types.Column{
			Name: col.Name,
			Type: dataType,
		}
	}

	if err := e.db.CreateTable(stmt.TableName, schema); err != nil {
		return "", err
	}

	return fmt.Sprintf("Table '%s' created successfully", stmt.TableName), nil
}

func (e *Executor) executeInsert(stmt *parser.Insert) (string, error) {
	table, exists := e.db.Tables[stmt.TableName]
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", stmt.TableName)
	}

	if len(stmt.Values) != len(table.Schema) {
		return "", fmt.Errorf("column count mismatch: expected %d, got %d",
			len(table.Schema), len(stmt.Values))
	}

	row := make(types.Row, len(stmt.Values))
	for i, val := range stmt.Values {
		row[i] = val.ToInterface()
	}

	table.Insert(row)
	return fmt.Sprintf("Inserted 1 row into '%s'", stmt.TableName), nil
}

func (e *Executor) executeSelect(stmt *parser.Select) (string, error) {
	table, exists := e.db.Tables[stmt.TableName]
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", stmt.TableName)
	}

	result := fmt.Sprintf("Results from '%s':\n", stmt.TableName)

	// Print header
	for i, col := range table.Schema {
		if i > 0 {
			result += " | "
		}
		result += col.Name
	}
	result += "\n" + strings.Repeat("-", len(result)) + "\n"

	// Print rows
	rowCount := 0
	table.Scan(func(row types.Row) bool {
		for i, val := range row {
			if i > 0 {
				result += " | "
			}
			result += fmt.Sprintf("%v", val)
		}
		result += "\n"
		rowCount++
		return true
	})

	result += fmt.Sprintf("\n%d row(s) returned", rowCount)
	return result, nil
}

func parseDataType(typeStr string) (types.DataType, error) {
	switch typeStr {
	case "INT":
		return types.INT, nil
	case "TEXT":
		return types.TEXT, nil
	case "BOOLEAN":
		return types.BOOLEAN, nil
	case "FLOAT":
		return types.FLOAT, nil
	default:
		return 0, fmt.Errorf("unknown data type: %s", typeStr)
	}
}
