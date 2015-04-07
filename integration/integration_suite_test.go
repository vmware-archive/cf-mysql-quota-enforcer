package enforcer_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/test_helpers"

	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"os"
	"os/exec"
)

var brokerDBName string
var rootConfig database.Config
var binaryPath string

func TestEnforcer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Enforcer Suite")
}

var _ = BeforeSuite(func() {
	initConfig := test_helpers.NewRootDatabaseConfig("")

	brokerDBName = fmt.Sprintf("quota_enforcer_integration_enforcer_test_%d", GinkgoParallelNode())
	rootConfig = test_helpers.NewRootDatabaseConfig(brokerDBName)

	initDB, err := database.NewDB(initConfig)
	Expect(err).ToNot(HaveOccurred())
	defer initDB.Close()

	_, err = initDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", brokerDBName))
	Expect(err).ToNot(HaveOccurred())

	db, err := database.NewDB(rootConfig)
	Expect(err).ToNot(HaveOccurred())
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS service_instances (
    id int(11) NOT NULL AUTO_INCREMENT,
    guid varchar(255),
    plan_guid varchar(255),
    max_storage_mb int(11) NOT NULL DEFAULT '0',
    db_name varchar(255),
    PRIMARY KEY (id)
	)`)
	Expect(err).ToNot(HaveOccurred())

	binaryPath, err = gexec.Build("github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer")
	Expect(err).ToNot(HaveOccurred())

	_, err = os.Stat(binaryPath)
	if err != nil {
		Expect(os.IsExist(err)).To(BeTrue())
	}
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()

	_, err := os.Stat(binaryPath)
	if err != nil {
		Expect(os.IsExist(err)).To(BeFalse())
	}

	var emptyConfig database.Config
	if rootConfig != emptyConfig {
		db, err := database.NewDB(rootConfig)
		Expect(err).ToNot(HaveOccurred())
		defer db.Close()

		_, err = db.Exec("DROP TABLE IF EXISTS service_instances")
		Expect(err).ToNot(HaveOccurred())

		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", brokerDBName))
		Expect(err).ToNot(HaveOccurred())

	}
})

func executeQuotaEnforcer() {
	command := exec.Command(
		binaryPath,
		fmt.Sprintf("-brokerDBName=%s", brokerDBName),
		"-logLevel=debug",
	)

	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ShouldNot(HaveOccurred())

	session.Wait(1 * time.Minute)
	Expect(session.ExitCode()).To(Equal(0), string(session.Err.Contents()))
}
