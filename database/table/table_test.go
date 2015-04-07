package table_test

import (
	"database/sql"
	"errors"

	. "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database/table"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/test_helpers"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Table", func() {

	Describe("Size", func() {
		const dbName = "fake-db-name"
		const tableName = "fake-table-name"
		var sizeQueryPattern = test_helpers.CompressWhitespace(
			`SELECT \(data_length \+ index_length\) 
      FROM information_schema\.TABLES 
			WHERE table_schema = \? AND table_name = \?`)
		var sizeColumns = []string{"size"}

		var (
			logger *lagertest.TestLogger
			table  Table
			fakeDB *sql.DB
		)

		BeforeEach(func() {
			var err error
			fakeDB, err = sqlmock.New()
			Expect(err).ToNot(HaveOccurred())

			logger = lagertest.NewTestLogger("Database Table test")
			table = New(dbName, tableName, fakeDB)
		})

		AfterEach(func() {
			err := fakeDB.Close()
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns table size in bytes", func() {
			sqlmock.ExpectQuery(sizeQueryPattern).
				WithArgs(dbName, tableName).
				WillReturnRows(sqlmock.NewRows(sizeColumns).AddRow(128 * 1024))

			sizeBytes, err := table.Size()
			Expect(err).ToNot(HaveOccurred())
			Expect(sizeBytes).To(Equal(int64(128 * 1024)))
		})

		Context("when the table does not exist", func() {
			It("returns an error", func() {
				sqlmock.ExpectQuery(sizeQueryPattern).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows(sizeColumns))

				_, err := table.Size()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no rows in result set"))
			})
		})

		Context("when the query errors", func() {
			It("returns an error", func() {
				sqlmock.ExpectQuery(sizeQueryPattern).
					WithArgs(dbName, tableName).
					WillReturnError(errors.New("fake-query-error"))

				_, err := table.Size()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-query-error"))
			})
		})
	})
})
