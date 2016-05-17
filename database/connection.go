package database

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	dbWaitTimeout = 60
)

func NewConnection(username, password, host string, port int, dbName string) (*sql.DB, error) {
	var count = 0
	var userPass string
	if password != "" {
		userPass = fmt.Sprintf("%s:%s", username, password)
	} else {
		userPass = username
	}

	dbConnection, _ := sql.Open("mysql", fmt.Sprintf(
		"%s@tcp(%s:%d)/%s",
		userPass,
		host,
		port,
		dbName,
	))

	//time.Sleep(dbWaitTimeout * time.Second)
	for dbConnection.Ping() != nil && count < dbWaitTimeout {
		count++
		time.Sleep(1 * time.Second)
	}

	if count <= dbWaitTimeout {
		fmt.Printf("Database startup took %d seconds", count)
	}

	if dbConnection.Ping() != nil { // error case
		return nil, errors.New("Could not open DB connection")
	} else {
		return dbConnection, nil
	}

}
