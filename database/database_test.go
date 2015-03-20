package database_test

import (
	. "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"errors"

	"database/sql"

	sqlfakes "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/sql/fakes"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("Database", func() {

	const dbName = "fake-db-name"
	var (
		logger   *lagertest.TestLogger
		database Database
		fakeDB   *sqlfakes.FakeDB
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("ProxyRunner test")
		fakeDB = &sqlfakes.FakeDB{}
		database = New(dbName, fakeDB, logger)
	})

	Describe("RevokePrivileges", func() {
		var fakeResult *sqlfakes.FakeResult

		BeforeEach(func() {
			fakeResult = &sqlfakes.FakeResult{}
			fakeDB.ExecReturns(fakeResult, nil)

			fakeResult.RowsAffectedReturns(1, nil)
		})

		It("makes a sql query to revoke priveledges on a database and then flushes privileges", func() {
			err := database.RevokePrivileges()
			Expect(err).ToNot(HaveOccurred())

			Expect(fakeDB.ExecCallCount()).To(Equal(2))

			query, args := fakeDB.ExecArgsForCall(0)
			Expect(query).To(Equal(`UPDATE mysql.db
SET Insert_priv = 'N', Update_priv = 'N', Create_priv = 'N'
WHERE Db = ?`))
			Expect(args).To(Equal([]interface{}{dbName}))

			query, args = fakeDB.ExecArgsForCall(1)
			Expect(query).To(Equal("FLUSH PRIVILEGES"))
			Expect(args).To(BeEmpty())
		})

		Context("when the query fails", func() {
			BeforeEach(func() {
				fakeDB.ExecReturns(nil, errors.New("fake-query-error"))
			})

			It("returns an error", func() {
				err := database.RevokePrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-query-error"))
				Expect(err.Error()).To(ContainSubstring(dbName))

				Expect(fakeDB.ExecCallCount()).To(Equal(1))
			})
		})

		Context("when getting the number of affected rows fails", func() {
			BeforeEach(func() {
				fakeResult.RowsAffectedReturns(0, errors.New("fake-rows-affected-error"))
			})

			It("returns an error", func() {
				err := database.RevokePrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-rows-affected-error"))
				Expect(err.Error()).To(ContainSubstring("Getting rows affected"))
				Expect(err.Error()).To(ContainSubstring(dbName))

				Expect(fakeDB.ExecCallCount()).To(Equal(1))
				Expect(fakeResult.RowsAffectedCallCount()).To(Equal(1))
			})
		})

		Context("when flushing privileges fails", func() {
			BeforeEach(func() {
				fakeDB.ExecStub = func(query string, args ...interface{}) (sql.Result, error) {
					if query == "FLUSH PRIVILEGES" {
						return nil, errors.New("fake-flush-error")
					}
					return fakeResult, nil
				}
			})

			It("returns an error", func() {
				err := database.RevokePrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-flush-error"))

				Expect(fakeDB.ExecCallCount()).To(Equal(2))
			})
		})
	})

	Describe("GrantPrivileges", func() {
		var fakeResult *sqlfakes.FakeResult

		BeforeEach(func() {
			fakeResult = &sqlfakes.FakeResult{}
			fakeDB.ExecReturns(fakeResult, nil)

			fakeResult.RowsAffectedReturns(1, nil)
		})

		It("Expects priviledges to be granted to the database", func() {
			err := database.GrantPrivileges()
			Expect(err).ToNot(HaveOccurred())

			Expect(fakeDB.ExecCallCount()).To(Equal(2))

			query, args := fakeDB.ExecArgsForCall(0)
			Expect(query).To(Equal(`UPDATE mysql.db
SET Insert_priv = 'Y', Update_priv = 'Y', Create_priv = 'Y'
WHERE Db = ?`))
			Expect(args).To(Equal([]interface{}{dbName}))

			query, args = fakeDB.ExecArgsForCall(1)
			Expect(query).To(Equal("FLUSH PRIVILEGES"))
			Expect(args).To(BeEmpty())
		})

		Context("when the query fails", func() {
			BeforeEach(func() {
				fakeDB.ExecReturns(nil, errors.New("fake-query-error"))
			})

			It("returns an error", func() {
				err := database.GrantPrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-query-error"))
				Expect(err.Error()).To(ContainSubstring(dbName))

				Expect(fakeDB.ExecCallCount()).To(Equal(1))
			})
		})

		Context("when getting the number of affected rows fails", func() {
			BeforeEach(func() {
				fakeResult.RowsAffectedReturns(0, errors.New("fake-rows-affected-error"))
			})

			It("returns an error", func() {
				err := database.GrantPrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-rows-affected-error"))
				Expect(err.Error()).To(ContainSubstring("Getting rows affected"))
				Expect(err.Error()).To(ContainSubstring(dbName))

				Expect(fakeDB.ExecCallCount()).To(Equal(1))
				Expect(fakeResult.RowsAffectedCallCount()).To(Equal(1))
			})
		})

		Context("when flushing privileges fails", func() {
			BeforeEach(func() {
				fakeDB.ExecStub = func(query string, args ...interface{}) (sql.Result, error) {
					if query == "FLUSH PRIVILEGES" {
						return nil, errors.New("fake-flush-error")
					}
					return fakeResult, nil
				}
			})

			It("returns an error", func() {
				err := database.GrantPrivileges()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("fake-flush-error"))

				Expect(fakeDB.ExecCallCount()).To(Equal(2))
			})
		})

	})

	Describe("ResetActivePrivileges", func() {

	})
})
