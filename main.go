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
	runOnce := flags.Bool("run-once", false, "Run only once instead of continuously")
	configFile := flags.String("configFile", "", "Location of config file")
	cf_lager.AddFlags(flags)
	flags.Parse(os.Args[1:])
	logger, _ := cf_lager.New("Quota Enforcer")

	logger.Info("Config file", lager.Data{"configFile": configFile})
	config, err := config.Load(*configFile)
	if err != nil {
		panic(err.Error())
	}

	logger.Info(
		"Database connection established.",
		lager.Data{
			"Host":         config.Host,
			"Port":         config.Port,
			"User":         config.User,
			"DatabaseName": config.DBName,
		})

	db, err := database.NewConnection(*config)
	if err != nil {
		panic(err.Error())
	}

	violatorRepo := database.NewViolatorRepo(config.DBName, db, logger)
	reformerRepo := database.NewReformerRepo(config.DBName, db, logger)

	e := enforcer.NewEnforcer(violatorRepo, reformerRepo)

	if *runOnce {
		logger.Info("Running once")
		enforce(e, logger)
	} else {
		logger.Info("Running continuously")
		func() {
			for {
				enforce(e, logger)
				time.Sleep(1 * time.Second)
			}
		}()
	}
}

func enforce(e enforcer.Enforcer, logger lager.Logger) {
	logger.Info("Enforcing")
	err := e.Enforce()
	if err != nil {
		logger.Info(fmt.Sprintf("Enforcing Failed: %s", err.Error()))
	}
}
