package database_test

import (
	. "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/test_helpers"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"database/sql"
	"fmt"

	"errors"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("ReformerRepo", func() {

	const brokerDBName = "fake_broker_db_name"
	var (
		logger *lagertest.TestLogger
		repo   Repo
		fakeDB *sql.DB
	)

	BeforeEach(func() {
		var err error
		fakeDB, err = sqlmock.New()
		Expect(err).ToNot(HaveOccurred())

		logger = lagertest.NewTestLogger("ReformerRepo test")
		repo = NewReformerRepo(brokerDBName, fakeDB, logger)
	})

	AfterEach(func() {
		err := fakeDB.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("All", func() {
		var (
			tableSchemaColumns    = []string{"db"}
			queryReformersPattern = test_helpers.CompressWhitespace(fmt.Sprintf(`SELECT tables.table_schema AS db
FROM   information_schema.tables AS tables
JOIN   \(
           SELECT DISTINCT dbs.Db AS Db from mysql.db AS dbs
           WHERE \(dbs.Insert_priv = 'N' OR dbs.Update_priv = 'N' OR dbs.Create_priv = 'N'\)
       \) AS dbs ON tables.table_schema = dbs.Db
JOIN   %s.service_instances AS instances ON tables.table_schema = instances.db_name COLLATE utf8_general_ci
GROUP  BY tables.table_schema
HAVING ROUND\(SUM\(tables.data_length \+ tables.index_length\) / 1024 / 1024, 1\) < MAX\(instances.max_storage_mb\)`,
				brokerDBName,
			))
		)

		It("returns a list of databases that have come under their quota", func() {
			sqlmock.ExpectQuery(queryReformersPattern).
				WithArgs().
				WillReturnRows(sqlmock.NewRows(tableSchemaColumns).AddRow("fake-database-1").AddRow("fake-database-2"))

			reformers, err := repo.All()
			Expect(err).ToNot(HaveOccurred())

			Expect(reformers).To(ConsistOf(
				New("fake-database-1", fakeDB, logger),
				New("fake-database-2", fakeDB, logger),
			))
		})

		Context("when there are no reformers", func() {
			BeforeEach(func() {
				sqlmock.ExpectQuery(queryReformersPattern).
					WithArgs().
					WillReturnRows(sqlmock.NewRows(tableSchemaColumns))
			})

			It("returns an empty list", func() {
				violators, err := repo.All()
				Expect(err).ToNot(HaveOccurred())

				Expect(violators).To(BeEmpty())
			})
		})

		Context("when the db query fails", func() {
			BeforeEach(func() {
				sqlmock.ExpectQuery(queryReformersPattern).
					WithArgs().
					WillReturnError(errors.New("fake-query-error"))
			})

			It("returns an error", func() {
				_, err := repo.All()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-query-error"))
			})
		})
	})
})
