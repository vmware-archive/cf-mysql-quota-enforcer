package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/enforcer"
	"github.com/pivotal-golang/lager"
)

func main() {
	logger := lager.NewLogger("Quota Enforcer")

	mysqlUser := "root"
	mysqlPassword := "password"
	databaseName := "development"
	host := "localhost"
	port := 3306

	logger.Info(fmt.Sprintf("Connection to database '%s' at '%s:%d' as '%s'", databaseName, host, port, mysqlUser))

	db, err := sql.Open(databaseName, fmt.Sprintf("%s:%s@%s:%d/", mysqlUser, mysqlPassword, host, port))
	if err != nil {
		panic(err.Error())
	}

	violatorRepo := database.NewViolatorRepo(databaseName, db, logger)
	reformerRepo := database.NewReformerRepo(databaseName, db, logger)

	e := enforcer.NewEnforcer(violatorRepo, reformerRepo)

	for {
		err = e.Enforce()
		if err != nil {
			panic(fmt.Sprintf("Enforcing Failed: %s", err.Error()))
		}

		time.Sleep(1 * time.Second)
	}
}
