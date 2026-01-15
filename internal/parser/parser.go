package parser

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// SQL is the top-level statement
type SQL struct {
	CreateTable *CreateTable `@@ ";"`
	Insert      *Insert      `| @@ ";"`
	Select      *Select      `| @@ ";"`
}

// CREATE TABLE users (id INT, name TEXT, is_admin BOOLEAN, score FLOAT)
type CreateTable struct {
	TableName string   `"CREATE" "TABLE" @Ident`
	Columns   []Column `"(" @@ ("," @@)* ")"`
}

type Column struct {
	Name string `@Ident`
	Type string `@("INT" | "TEXT" | "BOOLEAN" | "FLOAT")`
}

// INSERT INTO users VALUES (1, 'Trevor', true, 95.5)
type Insert struct {
	TableName string  `"INSERT" "INTO" @Ident`
	Values    []Value `"VALUES" "(" @@ ("," @@)* ")"`
}

type Value struct {
	Number  *float64 `  @(Int | Float)`
	String  *string  `| @String`
	Boolean *bool    `| (@"true" | "false")`
}

// SELECT * FROM users
type Select struct {
	TableName string `"SELECT" "*" "FROM" @Ident`
}

var (
	sqlLexer = lexer.MustSimple([]lexer.SimpleRule{
		{Name: "Keyword", Pattern: `(?i)\b(CREATE|TABLE|INSERT|INTO|VALUES|SELECT|FROM|INT|TEXT|BOOLEAN|FLOAT|true|false)\b`},
		{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
		{Name: "Float", Pattern: `\d+\.\d+`},
		{Name: "Int", Pattern: `\d+`},
		{Name: "String", Pattern: `'[^']*'`},
		{Name: "Punct", Pattern: `[(),*;]`},
		{Name: "whitespace", Pattern: `\s+`},
	})

	parser = participle.MustBuild[SQL](
		participle.Lexer(sqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Keyword"),
	)
)

func Parse(input string) (*SQL, error) {
	input = strings.TrimSpace(input)

	sql, err := parser.ParseString("", input)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	return sql, nil
}

// A helper to convert the  parsed value to an interface{}
func (v *Value) ToInterface() interface{} {
	if v.Number != nil {
		// Check if it's an integer
		if *v.Number == float64(int(*v.Number)) {
			return int(*v.Number)
		}
		return *v.Number
	}
	if v.String != nil {
		return *v.String
	}
	if v.Boolean != nil {
		return *v.Boolean
	}
	return nil
}
