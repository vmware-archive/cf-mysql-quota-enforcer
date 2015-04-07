package enforcer_test

import (
	"database/sql"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	_ "github.com/go-sql-driver/mysql"

	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database/table"
)

var _ = Describe("Enforcer Integration", func() {

	overflowDatabase := func(numRows int, tableName string, db *sql.DB) {
		_, err := db.Exec(fmt.Sprintf(
			`CREATE TABLE %s 
			(id MEDIUMINT AUTO_INCREMENT, data LONGBLOB, PRIMARY KEY (id))`,
			tableName,
		))
		Expect(err).NotTo(HaveOccurred())

		data := make([]byte, 1024*1024)
		for row := 0; row < numRows; row++ {
			_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", tableName), data)
			Expect(err).NotTo(HaveOccurred())
		}
	}

	var userConfig database.Config

	BeforeEach(func() {
		userConfig = database.Config{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     fmt.Sprintf("diff_user_guid_%d", GinkgoParallelNode()),
			Password: "fake_user_password",
			DBName:   fmt.Sprintf("fake_user_db_name_%d", GinkgoParallelNode()),
		}

	})

	Context("When the quota-enforcer is running", func() {
		Context("when a user database exists", func() {
			var (
				plan         = "fake_plan_guid"
				maxStorageMB = 3
				dataTable    = "data_table"
			)

			BeforeEach(func() {
				db, err := database.NewDB(rootConfig)
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
				db, err := database.NewDB(rootConfig)
				Expect(err).NotTo(HaveOccurred())
				defer db.Close()

				_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", userConfig.DBName, dataTable))
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
					db, err := database.NewDB(userConfig)
					Expect(err).NotTo(HaveOccurred())
					defer db.Close()

					overflowDatabase(maxStorageMB, dataTable, db)

					sizeBytes, err := table.New(userConfig.DBName, dataTable, db).Size()
					Expect(err).NotTo(HaveOccurred())
					Expect(sizeBytes).To(BeNumerically(">=", maxStorageMB*1024*1024))

					executeQuotaEnforcer()

					_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", dataTable), []byte{'1'})
					Expect(err).To(HaveOccurred())
				})

				By("Re-enabling write access when back under the quota", func() {
					db, err := database.NewDB(userConfig)
					Expect(err).NotTo(HaveOccurred())
					defer db.Close()

					// InnoDB storage reduces table size better when deleting from the end of the table.
					// For the record: InnoDB also uses about 0.52MB overhead per table.
					_, err = db.Exec(fmt.Sprintf("DELETE FROM %s ORDER BY id DESC LIMIT 1", dataTable))
					Expect(err).NotTo(HaveOccurred())

					sizeBytes, err := table.New(userConfig.DBName, dataTable, db).Size()
					Expect(err).NotTo(HaveOccurred())
					Expect(sizeBytes).To(BeNumerically("<", maxStorageMB*1024*1024))

					executeQuotaEnforcer()

					_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", dataTable), []byte{'1'})
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})
	})
})
