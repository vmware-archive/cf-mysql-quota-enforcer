package table

import (
	"database/sql"
)

type Table interface {
	Size() (int64, error)
}

type table struct {
	dbName    string
	tableName string
	db        *sql.DB
}

func New(dbName, tableName string, db *sql.DB) Table {
	return &table{
		dbName:    dbName,
		tableName: tableName,
		db:        db,
	}
}

// Size returns a table's size in bytes according to its metadata
func (t table) Size() (int64, error) {
	var sizeBytes int64
	err := t.db.QueryRow(
		`SELECT (data_length + index_length) 
      FROM information_schema.TABLES 
      WHERE table_schema = ? AND table_name = ?`,
		t.dbName,
		t.tableName,
	).Scan(&sizeBytes)
	return sizeBytes, err
}
