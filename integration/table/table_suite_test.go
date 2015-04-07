package table_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/test_helpers"

	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

var tableDBName string
var rootConfig database.Config

func TestEnforcer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Table Suite")
}

var _ = BeforeSuite(func() {
	tableDBName = fmt.Sprintf("quota_enforcer_integration_table_test_%d", GinkgoParallelNode())
	rootConfig = test_helpers.NewRootDatabaseConfig(tableDBName)

	initConfig := test_helpers.NewRootDatabaseConfig("")

	db, err := database.NewDB(initConfig)
	Expect(err).ToNot(HaveOccurred())
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", tableDBName))
	Expect(err).ToNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	db, err := database.NewDB(rootConfig)
	Expect(err).ToNot(HaveOccurred())
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", tableDBName))
	Expect(err).ToNot(HaveOccurred())
})
