package database_test

import (
	. "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"errors"

	"database/sql"

	"github.com/DATA-DOG/go-sqlmock"
	sqlfakes "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/sql/fakes"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("Database", func() {

	const dbName = "fake-db-name"
	var (
		logger                 *lagertest.TestLogger
		database               Database
		fakeDB                 *sql.DB
		flushPrivilegesPattern = "FLUSH PRIVILEGES"
	)

	BeforeEach(func() {
		var err error
		fakeDB, err = sqlmock.New()
		Expect(err).ToNot(HaveOccurred())

		logger = lagertest.NewTestLogger("ProxyRunner test")
		database = New(dbName, fakeDB, logger)
	})

	AfterEach(func() {
		err := fakeDB.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("RevokePrivileges", func() {
		var (
			fakeResult              *sqlfakes.FakeResult
			revokePrivilegesPattern = "UPDATE mysql.db SET Insert_priv = 'N', Update_priv = 'N', Create_priv = 'N' WHERE Db = \\?"
		)

		BeforeEach(func() {
			fakeResult = &sqlfakes.FakeResult{}
			fakeResult.RowsAffectedReturns(1, nil)
		})

		It("makes a sql query to revoke priveledges on a database and then flushes privileges", func() {
			sqlmock.ExpectExec(revokePrivilegesPattern).
				WithArgs(dbName).
				WillReturnResult(fakeResult)

			sqlmock.ExpectExec(flushPrivilegesPattern).
				WithArgs().
				WillReturnResult(&sqlfakes.FakeResult{})

			err := database.RevokePrivileges()
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when the query fails", func() {
			BeforeEach(func() {
				sqlmock.ExpectExec(revokePrivilegesPattern).
					WithArgs(dbName).
					WillReturnError(errors.New("fake-query-error"))
			})

			It("returns an error", func() {
				err := database.RevokePrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-query-error"))
				Expect(err.Error()).To(ContainSubstring(dbName))
			})
		})

		Context("when getting the number of affected rows fails", func() {
			BeforeEach(func() {
				sqlmock.ExpectExec(revokePrivilegesPattern).
					WithArgs(dbName).
					WillReturnResult(fakeResult)

				fakeResult.RowsAffectedReturns(0, errors.New("fake-rows-affected-error"))
			})

			It("returns an error", func() {
				err := database.RevokePrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-rows-affected-error"))
				Expect(err.Error()).To(ContainSubstring("Getting rows affected"))
				Expect(err.Error()).To(ContainSubstring(dbName))

				Expect(fakeResult.RowsAffectedCallCount()).To(Equal(1))
			})
		})

		Context("when flushing privileges fails", func() {
			BeforeEach(func() {
				sqlmock.ExpectExec(revokePrivilegesPattern).
					WithArgs(dbName).
					WillReturnResult(fakeResult)

				sqlmock.ExpectExec(flushPrivilegesPattern).
					WithArgs().
					WillReturnError(errors.New("fake-flush-error"))
			})

			It("returns an error", func() {
				err := database.RevokePrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-flush-error"))
			})
		})
	})

	Describe("GrantPrivileges", func() {
		var (
			fakeResult             *sqlfakes.FakeResult
			grantPrivilegesPattern = "UPDATE mysql.db SET Insert_priv = 'Y', Update_priv = 'Y', Create_priv = 'Y' WHERE Db = \\?"
		)

		BeforeEach(func() {
			fakeResult = &sqlfakes.FakeResult{}
			fakeResult.RowsAffectedReturns(1, nil)
		})

		It("grants priviledges to the database", func() {
			sqlmock.ExpectExec(grantPrivilegesPattern).
				WithArgs(dbName).
				WillReturnResult(fakeResult)

			sqlmock.ExpectExec(flushPrivilegesPattern).
				WithArgs().
				WillReturnResult(&sqlfakes.FakeResult{})

			err := database.GrantPrivileges()
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when the query fails", func() {
			BeforeEach(func() {
				sqlmock.ExpectExec(grantPrivilegesPattern).
					WithArgs(dbName).
					WillReturnError(errors.New("fake-query-error"))
			})

			It("returns an error", func() {
				err := database.GrantPrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-query-error"))
				Expect(err.Error()).To(ContainSubstring(dbName))
			})
		})

		Context("when getting the number of affected rows fails", func() {
			BeforeEach(func() {
				sqlmock.ExpectExec(grantPrivilegesPattern).
					WithArgs(dbName).
					WillReturnResult(fakeResult)

				fakeResult.RowsAffectedReturns(0, errors.New("fake-rows-affected-error"))
			})

			It("returns an error", func() {
				err := database.GrantPrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-rows-affected-error"))
				Expect(err.Error()).To(ContainSubstring("Getting rows affected"))
				Expect(err.Error()).To(ContainSubstring(dbName))

				Expect(fakeResult.RowsAffectedCallCount()).To(Equal(1))
			})
		})

		Context("when flushing privileges fails", func() {
			BeforeEach(func() {
				sqlmock.ExpectExec(grantPrivilegesPattern).
					WithArgs(dbName).
					WillReturnResult(fakeResult)

				sqlmock.ExpectExec(flushPrivilegesPattern).
					WithArgs().
					WillReturnError(errors.New("fake-flush-error"))
			})

			It("returns an error", func() {
				err := database.GrantPrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-flush-error"))
			})
		})

	})

	Describe("KillActiveConnections", func() {
		var (
			fakeResult            *sqlfakes.FakeResult
			processListColumns    = []string{"ID"}
			processQueryPattern   = "SELECT ID FROM INFORMATION_SCHEMA.PROCESSLIST WHERE DB = \\? AND USER <> 'root'"
			killConnectionPattern = "KILL CONNECTION \\?"
		)

		BeforeEach(func() {
			fakeResult = &sqlfakes.FakeResult{}
			fakeResult.RowsAffectedReturns(1, nil)
		})

		It("kills all active connections to DB", func() {
			sqlmock.ExpectQuery(processQueryPattern).
				WithArgs(dbName).
				WillReturnRows(sqlmock.NewRows(processListColumns).AddRow(1).AddRow(123))

			sqlmock.ExpectExec(killConnectionPattern).
				WithArgs(1).
				WillReturnResult(&sqlfakes.FakeResult{})

			sqlmock.ExpectExec(killConnectionPattern).
				WithArgs(123).
				WillReturnResult(&sqlfakes.FakeResult{})

			err := database.KillActiveConnections()
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when there are no active connections to the database", func() {
			It("does not kill any connections", func() {
				sqlmock.ExpectQuery(processQueryPattern).
					WithArgs(dbName).
					WillReturnRows(sqlmock.NewRows(processListColumns))

				err := database.KillActiveConnections()
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when there is only one active connections to the database", func() {
			It("kills the active connection", func() {
				sqlmock.ExpectQuery(processQueryPattern).
					WithArgs(dbName).
					WillReturnRows(sqlmock.NewRows(processListColumns).AddRow(123))

				sqlmock.ExpectExec(killConnectionPattern).
					WithArgs(123).
					WillReturnResult(&sqlfakes.FakeResult{})

				err := database.KillActiveConnections()
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when querying for active connections fails", func() {
			BeforeEach(func() {
				sqlmock.ExpectQuery(processQueryPattern).
					WithArgs(dbName).
					WillReturnError(errors.New("fake-query-error"))
			})

			It("returns an error", func() {
				err := database.KillActiveConnections()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-query-error"))
				Expect(err.Error()).To(ContainSubstring(dbName))
			})
		})

		Context("when killing a connection fails", func() {
			BeforeEach(func() {
				sqlmock.ExpectQuery(processQueryPattern).
					WithArgs(dbName).
					WillReturnRows(sqlmock.NewRows(processListColumns).AddRow(1).AddRow(2).AddRow(3))

				sqlmock.ExpectExec(killConnectionPattern).
					WithArgs(2).
					WillReturnError(errors.New("fake-exec-error"))
			})

			It("kills all other active connections", func() {
				sqlmock.ExpectExec(killConnectionPattern).
					WithArgs(1).
					WillReturnResult(&sqlfakes.FakeResult{})

				sqlmock.ExpectExec(killConnectionPattern).
					WithArgs(3).
					WillReturnResult(&sqlfakes.FakeResult{})

				err := database.KillActiveConnections()
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})
})
