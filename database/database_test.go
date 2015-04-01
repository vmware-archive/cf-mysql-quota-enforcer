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
		logger   *lagertest.TestLogger
		database Database
		fakeDB   *sql.DB
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
		var fakeResult *sqlfakes.FakeResult

		BeforeEach(func() {
			fakeResult = &sqlfakes.FakeResult{}
			fakeResult.RowsAffectedReturns(1, nil)
		})

		It("makes a sql query to revoke priveledges on a database and then flushes privileges", func() {
			sqlmock.ExpectExec("UPDATE mysql.db SET Insert_priv = 'N', Update_priv = 'N', Create_priv = 'N' WHERE Db = ?").
                WithArgs(dbName).
                WillReturnResult(fakeResult)

			sqlmock.ExpectExec("FLUSH PRIVILEGES").
                WithArgs().
                WillReturnResult(&sqlfakes.FakeResult{})

			err := database.RevokePrivileges()
			Expect(err).ToNot(HaveOccurred())
		})

        Context("when the query fails", func() {
            BeforeEach(func() {
                sqlmock.ExpectExec("UPDATE mysql.db SET Insert_priv = 'N', Update_priv = 'N', Create_priv = 'N' WHERE Db = ?").
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
                sqlmock.ExpectExec("UPDATE mysql.db SET Insert_priv = 'N', Update_priv = 'N', Create_priv = 'N' WHERE Db = ?").
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
                sqlmock.ExpectExec("UPDATE mysql.db SET Insert_priv = 'N', Update_priv = 'N', Create_priv = 'N' WHERE Db = ?").
                    WithArgs(dbName).
                    WillReturnResult(fakeResult)

                sqlmock.ExpectExec("FLUSH PRIVILEGES").
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
        var fakeResult *sqlfakes.FakeResult

        BeforeEach(func() {
            fakeResult = &sqlfakes.FakeResult{}
            fakeResult.RowsAffectedReturns(1, nil)
        })

        It("grants priviledges to the database", func() {
            sqlmock.ExpectExec("UPDATE mysql.db SET Insert_priv = 'Y', Update_priv = 'Y', Create_priv = 'Y' WHERE Db = ?").
                WithArgs(dbName).
                WillReturnResult(fakeResult)

            sqlmock.ExpectExec("FLUSH PRIVILEGES").
                WithArgs().
                WillReturnResult(&sqlfakes.FakeResult{})

            err := database.GrantPrivileges()
            Expect(err).ToNot(HaveOccurred())
        })

        Context("when the query fails", func() {
            BeforeEach(func() {
                sqlmock.ExpectExec("UPDATE mysql.db SET Insert_priv = 'Y', Update_priv = 'Y', Create_priv = 'Y' WHERE Db = ?").
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
                sqlmock.ExpectExec("UPDATE mysql.db SET Insert_priv = 'Y', Update_priv = 'Y', Create_priv = 'Y' WHERE Db = ?").
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
                sqlmock.ExpectExec("UPDATE mysql.db SET Insert_priv = 'Y', Update_priv = 'Y', Create_priv = 'Y' WHERE Db = ?").
                    WithArgs(dbName).
                    WillReturnResult(fakeResult)

                sqlmock.ExpectExec("FLUSH PRIVILEGES").
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

	Describe("ResetActivePrivileges", func() {

	})
})
