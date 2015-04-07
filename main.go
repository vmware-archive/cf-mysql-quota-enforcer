package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/cloudfoundry-incubator/cf-lager"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/config"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/enforcer"
	"github.com/pivotal-golang/lager"
)

type Config struct {
	Host         string
	Port         int
	User         string
	Password     string
	BrokerDBName string
}

func main() {
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	daemonize := flags.Bool("d", false, "Daemonize process")
	brokerDBName := flags.String("brokerDBName", "", "Broker database name (overrides config file)")
	cf_lager.AddFlags(flags)
	flags.Parse(os.Args[1:])
	logger, _ := cf_lager.New("Quota Enforcer")

	configPath := os.Getenv("CONFIG")
	if configPath == "" {
		panic("CONFIG path must be specified")
	}

	config, err := config.Load(configPath)
	if err != nil {
		panic(err.Error())
	}

	databaseName := config.BrokerDBName
	if *brokerDBName != "" {
		databaseName = *brokerDBName
	}

	logger.Info(fmt.Sprintf(
		"Connection to database '%s' at '%s:%d' as '%s'",
		databaseName, config.Host, config.Port, config.User,
	))

	db, err := database.NewDB(database.Config{
		Host:     config.Host,
		Port:     config.Port,
		User:     config.User,
		Password: config.Password,
		DBName:   databaseName,
	})
	if err != nil {
		panic(err.Error())
	}

	violatorRepo := database.NewViolatorRepo(databaseName, db, logger)
	reformerRepo := database.NewReformerRepo(databaseName, db, logger)

	e := enforcer.NewEnforcer(violatorRepo, reformerRepo)

	if *daemonize {
		logger.Info("Running in daemonize mode")
		func() {
			for {
				enforce(e, logger)
				time.Sleep(1 * time.Second)
			}
		}()
	} else {
		logger.Info("Running once")
		enforce(e, logger)
	}
}

func enforce(e enforcer.Enforcer, logger lager.Logger) {
	logger.Info("Enforcing")
	err := e.Enforce()
	if err != nil {
		logger.Info(fmt.Sprintf("Enforcing Failed: %s", err.Error()))
	}
}
