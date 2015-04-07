package table_test

import (
	"database/sql"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	_ "github.com/go-sql-driver/mysql"

	. "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database/table"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/test_helpers"
)

var _ = Describe("Table Integration", func() {

	Describe("Size", func() {
		insert1MBRows := func(numRows int, tableName string, db *sql.DB) {
			data := make([]byte, 1024*1024)
			for row := 0; row < numRows; row++ {
				_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", tableName), data)
				Expect(err).NotTo(HaveOccurred())
			}
		}

		var (
			db        *sql.DB
			table     Table
			tableName string
		)

		BeforeEach(func() {
			tableName = fmt.Sprintf("table_size_test_%d", GinkgoParallelNode())

			var err error
			db, err = test_helpers.NewDB(rootConfig)
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Exec(fmt.Sprintf(
				`CREATE TABLE %s 
			(id MEDIUMINT AUTO_INCREMENT, data LONGBLOB, PRIMARY KEY (id))`,
				tableName,
			))
			Expect(err).NotTo(HaveOccurred())

			table = New(rootConfig.DBName, tableName, db)
		})

		AfterEach(func() {
			_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
			Expect(err).NotTo(HaveOccurred())

			if db != nil {
				Expect(db.Close()).ToNot(HaveOccurred())
			}
		})

		It("Returns the size of the table", func() {
			sizeBytes, err := table.Size()
			Expect(err).NotTo(HaveOccurred())
			Expect(sizeBytes).To(BeNumerically("<", 1024*1024))

			insert1MBRows(1, tableName, db)

			sizeBytes, err = table.Size()
			Expect(err).NotTo(HaveOccurred())
			Expect(sizeBytes).To(BeNumerically(">=", 1024*1024))
			Expect(sizeBytes).To(BeNumerically("<", 2*1024*1024))
		})
	})
})
