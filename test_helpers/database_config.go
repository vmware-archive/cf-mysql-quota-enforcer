package test_helpers

import (
	"os"

	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/config"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
)

func NewRootDatabaseConfig(dbName string) database.Config {
	configPath := os.Getenv("CONFIG")
	if configPath == "" {
		panic("CONFIG path must be specified")
	}

	config, err := config.Load(configPath)
	if err != nil {
		panic(err.Error())
	}

	return database.Config{
		Host:     config.Host,
		Port:     config.Port,
		User:     config.User,
		Password: config.Password,
		DBName:   dbName,
	}
}
