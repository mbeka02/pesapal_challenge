# Simple Go RDBMS

A lightweight, persistent relational database written in Go with a SQL-like REPL interface.

## Quick Start

```bash
go mod tidy
go run main.go
```

## SQL Syntax

The database supports a subset of SQL:

### Create Table
```sql
CREATE TABLE users (id INT, name TEXT, is_admin BOOLEAN, score FLOAT);
```

### Insert Data
```sql
INSERT INTO users VALUES (1, 'Alice', true, 95.5);
```

### Select Data
```sql
SELECT * FROM users;
```

## Supported Types

- `INT` (64-bit)
- `FLOAT` (64-bit)
- `TEXT` (String)
- `BOOLEAN` (true/false)

## Architecture

- **Parser:** SQL parsing via `participle`.
- **Executor:** Executes commands against the DB engine.
- **Storage:** Page-based persistence (4KB pages) with Heap file organization and Slotted Page layout.