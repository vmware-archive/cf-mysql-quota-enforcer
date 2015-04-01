package database_test

import (
	. "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"database/sql"
	"fmt"
	"regexp"

	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("ViolatorRepo", func() {

	var stripExtraSpace = func(in string) string {
		pattern := regexp.MustCompile("\\s+")
		return pattern.ReplaceAllString(in, " ")
	}

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

		logger = lagertest.NewTestLogger("ViolatorRepo test")
		repo = NewViolatorRepo(brokerDBName, fakeDB, logger)
	})

	AfterEach(func() {
		err := fakeDB.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("All", func() {
		var (
			tableSchemaColumns    = []string{"db"}
			queryViolatorsPattern = stripExtraSpace(fmt.Sprintf(`SELECT tables.table_schema AS db
FROM   information_schema.tables AS tables
JOIN   \(
           SELECT DISTINCT dbs.Db AS Db from mysql.db AS dbs
           WHERE \(dbs.Insert_priv = 'Y' OR dbs.Update_priv = 'Y' OR dbs.Create_priv = 'Y'\)
       \) AS dbs ON tables.table_schema = dbs.Db
JOIN   %s.service_instances AS instances ON tables.table_schema = instances.db_name COLLATE utf8_general_ci
GROUP  BY tables.table_schema
HAVING ROUND\(SUM\(tables.data_length \+ tables.index_length\) / 1024 / 1024, 1\) >= MAX\(instances.max_storage_mb\)`,
				brokerDBName,
			))
		)

		It("returns a list of databases that have exceeded their quota", func() {
			sqlmock.ExpectQuery(queryViolatorsPattern).
				WithArgs().
				WillReturnRows(sqlmock.NewRows(tableSchemaColumns).AddRow("fake-database-1").AddRow("fake-database-2"))

			violators, err := repo.All()
			Expect(err).ToNot(HaveOccurred())

			Expect(violators).To(ConsistOf(
				New("fake-database-1", fakeDB, logger),
				New("fake-database-2", fakeDB, logger),
			))
		})

		Context("when there are no violators", func() {
			BeforeEach(func() {
				sqlmock.ExpectQuery(queryViolatorsPattern).
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
				sqlmock.ExpectQuery(queryViolatorsPattern).
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
