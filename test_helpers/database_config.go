package test_helpers

import "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"

func NewRootDatabaseConfig(dbName string) database.Config {
	return database.Config{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "password",
		DBName:   dbName,
	}
}
