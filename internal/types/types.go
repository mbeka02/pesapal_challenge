package types

type DataType int

const (
	INT DataType = iota
	TEXT
	BOOLEAN
	FLOAT
)

type Column struct {
	Name string
	Type DataType
}

type (
	Value interface{}
	Row   []Value
)
