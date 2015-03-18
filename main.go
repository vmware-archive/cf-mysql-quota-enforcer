package main

import (
    "database/sql"
    "fmt"
    "time"

    "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
    "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/enforcer"
)

func main() {
    mysqlUser := ""
    mysqlPassword := ""
    databaseName := ""
	_, err := sql.Open(databaseName, fmt.Sprintf("%s:%s@/", mysqlUser, mysqlPassword))
    if err != nil {
        panic(err.Error())
    }

    //TODO: pass in dbConn
    violatorRepo := database.NewViolatorRepo()
    reformerRepo := database.NewReformerRepo()

	e := enforcer.NewEnforcer(violatorRepo, reformerRepo)

    for {
        e.Enforce()
        time.Sleep(1 * time.Second)
    }
}
