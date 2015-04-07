package test_helpers

import (
	"database/sql"
	"fmt"
)

type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

func NewRootDatabaseConfig(dbName string) DatabaseConfig {
	return DatabaseConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "password",
		DBName:   dbName,
	}
}

func NewDB(dbConfig DatabaseConfig) (*sql.DB, error) {
	return sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.DBName,
	))
}
