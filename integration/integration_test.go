package enforcer_test

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"

	_ "github.com/go-sql-driver/mysql"

	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/config"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
)

var _ = Describe("Enforcer Integration", func() {

	createSizedTable := func(numRows int, tableName string, db *sql.DB) {
		_, err := db.Exec(fmt.Sprintf(
			`CREATE TABLE %s 
			(id MEDIUMINT AUTO_INCREMENT, data LONGBLOB, PRIMARY KEY (id))
			ENGINE = INNODB`,
			tableName,
		))
		Expect(err).NotTo(HaveOccurred())

		data := make([]byte, 1024*1024)
		for row := 0; row < numRows; row++ {
			_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", tableName), data)
			Expect(err).NotTo(HaveOccurred())
		}
	}

	var userConfig config.Config

	BeforeEach(func() {
		userConfig = config.Config{
			Host:     rootConfig.Host,
			Port:     rootConfig.Port,
			User:     uuidWithUnderscores("user")[:16], // MySQL mandates usernames are <= 16 chars
			Password: uuidWithUnderscores("password"),
			DBName:   uuidWithUnderscores("db"),
		}
		err := userConfig.Validate()
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Writing pid file", func() {
		Context("when the quota enforcer is running continuously", func() {
			var (
				session     *gexec.Session
				pidFile     string
				pidFileFlag string
			)

			Context("when the pid file location is valid", func() {
				BeforeEach(func() {
					pidFile = fmt.Sprintf("%s/enforcer.pid", tempDir)
					pidFileFlag = fmt.Sprintf("-pidFile=%s", pidFile)
				})

				It("writes its pid to the provided file", func() {
					Expect(fileExists(pidFile)).To(BeFalse())
					session = runEnforcerContinuously(pidFileFlag)
					Expect(fileExists(pidFile)).To(BeTrue())
				})

				AfterEach(func() {
					session.Kill()

					// Once signalled, the session should shut down relatively quickly
					session.Wait(5 * time.Second)

					// We don't care what the exit code is
					Eventually(session).Should(gexec.Exit())
				})
			})

			Context("when the pid file location is invalid", func() {
				BeforeEach(func() {
					pidFile = "/invalid_path/enforcer.pid"
					pidFileFlag = fmt.Sprintf("-pidFile=%s", pidFile)
				})

				It("exits with error", func() {
					session = runEnforcerContinuously(pidFileFlag)

					Eventually(session.Err).Should(gbytes.Say(pidFile))
					Eventually(session).Should(gexec.Exit())
					Expect(session.ExitCode()).ToNot(Equal(0))
				})
			})
		})
	})

	Describe("Signal handling", func() {
		Context("when the quota enforcer is running continuously", func() {
			var session *gexec.Session

			BeforeEach(func() {
				session = runEnforcerContinuously()
			})

			It("shuts down on any signal", func() {
				session.Kill()

				// Once signalled, the session should shut down relatively quickly
				session.Wait(5 * time.Second)

				// We don't care what the exit code is
				Eventually(session).Should(gexec.Exit())
			})
		})
	})

	Describe("Quota enforcement", func() {
		var (
			plan          string
			maxStorageMB  int
			dataTableName string
			tempTableName string
		)

		BeforeEach(func() {
			plan = uuidWithUnderscores("plan")
			maxStorageMB = 10
			dataTableName = uuidWithUnderscores("data")
			tempTableName = uuidWithUnderscores("temp")
		})

		Context("when a user database exists", func() {
			BeforeEach(func() {
				db, err := database.NewConnection(rootConfig)
				Expect(err).NotTo(HaveOccurred())
				defer db.Close()

				_, err = db.Exec(fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s'", userConfig.User, userConfig.Password))
				Expect(err).NotTo(HaveOccurred())

				_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", userConfig.DBName))
				Expect(err).NotTo(HaveOccurred())

				_, err = db.Exec("INSERT INTO service_instances (guid,plan_guid,max_storage_mb,db_name) VALUES(?,?,?,?)", userConfig.User, plan, maxStorageMB, userConfig.DBName)
				Expect(err).NotTo(HaveOccurred())

				_, err = db.Exec(fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO %s", userConfig.DBName, userConfig.User))
				Expect(err).NotTo(HaveOccurred())

				_, err = db.Exec("FLUSH PRIVILEGES")
				Expect(err).NotTo(HaveOccurred())
			})

			AfterEach(func() {
				db, err := database.NewConnection(rootConfig)
				Expect(err).NotTo(HaveOccurred())
				defer db.Close()

				_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", userConfig.DBName, dataTableName))
				Expect(err).NotTo(HaveOccurred())

				_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", userConfig.DBName, tempTableName))
				Expect(err).NotTo(HaveOccurred())

				_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", userConfig.DBName))
				Expect(err).NotTo(HaveOccurred())

				_, err = db.Exec(fmt.Sprintf(`REVOKE ALL PRIVILEGES, GRANT OPTION FROM %s`, userConfig.User))
				Expect(err).NotTo(HaveOccurred())

				_, err = db.Exec(fmt.Sprintf("DROP USER %s", userConfig.User))
				Expect(err).NotTo(HaveOccurred())

				_, err = db.Exec("FLUSH PRIVILEGES")
				Expect(err).NotTo(HaveOccurred())
			})

			It("Enforces the quota", func() {
				By("Revoking write access when over the quota", func() {
					db, err := database.NewConnection(userConfig)
					Expect(err).NotTo(HaveOccurred())
					defer db.Close()

					createSizedTable(maxStorageMB/2, dataTableName, db)
					createSizedTable(maxStorageMB/2, tempTableName, db)

					runEnforcerOnce()

					_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", dataTableName), []byte{'1'})
					Expect(err).To(HaveOccurred())
				})

				By("Re-enabling write access when back under the quota", func() {
					db, err := database.NewConnection(userConfig)
					Expect(err).NotTo(HaveOccurred())
					defer db.Close()

					_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", tempTableName))
					Expect(err).NotTo(HaveOccurred())

					runEnforcerOnce()

					_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", dataTableName), []byte{'1'})
					Expect(err).NotTo(HaveOccurred())
				})
			})

			It("restores write access after dropping all tables", func() {
				db, err := database.NewConnection(userConfig)
				Expect(err).NotTo(HaveOccurred())
				defer db.Close()

				By("Revoking write access when over quota", func() {

					createSizedTable(maxStorageMB, dataTableName, db)

					runEnforcerOnce()

					_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", dataTableName), []byte{'1'})
					Expect(err).To(HaveOccurred())
				})

				By("Re-enabling write access when back under the quota", func() {
					_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", dataTableName))
					Expect(err).NotTo(HaveOccurred())

					runEnforcerOnce()

					createSizedTable(maxStorageMB/2, dataTableName, db)
					_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", dataTableName), []byte{'1'})
					Expect(err).NotTo(HaveOccurred())
				})

			})
		})
	})
})

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}
