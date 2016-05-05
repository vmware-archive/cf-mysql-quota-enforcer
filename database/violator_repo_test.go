package database_test

import (
	. "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"database/sql"

	"errors"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("ViolatorRepo", func() {

	const (
		brokerDBName = "fake_broker_db_name"
		adminUser    = "fake_admin_user"
		readOnlyUser = "fake_read_only_user"
	)

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
		repo = NewViolatorRepo(brokerDBName, adminUser, readOnlyUser, fakeDB, logger)
	})

	AfterEach(func() {
		err := fakeDB.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("All", func() {
		var (
			tableSchemaColumns = []string{"db", "user"}
			matchAny           = ".*"
		)

		It("returns a list of databases that have exceeded their quota", func() {
			sqlmock.ExpectQuery(matchAny).
				WithArgs().
				WillReturnRows(sqlmock.NewRows(tableSchemaColumns).
					AddRow("fake-database-1", "cf_fake-user-1").
					AddRow("fake-database-2", "cf_fake-user-2"))

			violators, err := repo.All()
			Expect(err).ToNot(HaveOccurred())

			Expect(violators).To(ConsistOf(
				New("fake-database-1", "cf_fake-user-1", fakeDB, logger),
				New("fake-database-2", "cf_fake-user-2", fakeDB, logger),
			))
		})

		Context("when there are no violators", func() {
			BeforeEach(func() {
				sqlmock.ExpectQuery(matchAny).
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
				sqlmock.ExpectQuery(matchAny).
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
