# Simple Go RDBMS

A lightweight, persistent relational database management system written in Go.

## Features

### Storage Engine

- **Page-Based Storage:** Uses fixed-size 4096B/4KB pages for data persistence.
- **Pager:** Manages reading and writing fixed-size pages to and from persistent storage.
- **Heap File Organization:** Tables are stored as heap files.
- **Slotted Page Layout:**
  - Pages utilize a Slotted Page structure to efficiently store variable-length records.
  - Supports multiple rows per page.
  - Manages free space dynamically within the page.

### Data Types

The database currently supports the following primitive data types:

- `INT` (64-bit integer)
- `FLOAT` (64-bit floating point)
- `TEXT` (Variable length string)
- `BOOLEAN` (True/False)

### Core Operations

- **Insert:** Rows are serialized into binary format and inserted into the first available page with sufficient space.
- **Scan:** Supports full table scans to iterate over all stored rows, decoding them back into structured data.

## Usage

### Example

Check `main.go` for a runnable example.

```go
// Define a table schema
users := &db.Table{
    Name: "users",
    Schema: []types.Column{
        {Name: "id", Type: types.INT},
        {Name: "name", Type: types.TEXT},
        {Name: "is_admin", Type: types.BOOLEAN},
        {Name: "score", Type: types.FLOAT},
    },
    Heap: heap,
}

// Insert data
users.Insert(types.Row{1, "Trevor", true, 95.5})

// Scan data
users.Scan(func(row types.Row) bool {
    fmt.Printf("Row: %v\n", row)
    return true // continue scanning
})
```

## Internal Architecture

### File Format

- **Page Header:** Contains metadata like the number of cells (rows) and the start offset of the data area.
- **Slot Array:** Grows forward from the header. Contains pointers (offset and length) to the actual row data.
- **Data Area:** Grows backward from the end of the page. Stores the serialized row bytes.
- **Free Space:** The area between the Slot Array and the Data Area.

### Serialization

Rows are encoded into a compact binary format:

- `INT`: 8 bytes (LittleEndian)
- `FLOAT`: 8 bytes (LittleEndian)
- `BOOLEAN`: 1 byte (0 or 1)
- `TEXT`: 4-byte length prefix + raw bytes
