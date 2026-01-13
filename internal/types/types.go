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

type TableMeta struct {
	Name      string
	Schema    []Column
	FirstPage uint64
}
type (
	Value interface{}
	Row   []Value
)
