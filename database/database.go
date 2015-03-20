package database

import (
	"fmt"

	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/sql"
	"github.com/pivotal-golang/lager"
)

const revokeQuery = `UPDATE mysql.db
SET Insert_priv = 'N', Update_priv = 'N', Create_priv = 'N'
WHERE Db = ?`

const grantQuery = `UPDATE mysql.db
SET Insert_priv = 'Y', Update_priv = 'Y', Create_priv = 'Y'
WHERE Db = ?`

type Database interface {
	GrantPrivileges() error
	RevokePrivileges() error
	ResetActivePrivileges() error
}

type database struct {
	name   string
	db     sql.DB
	logger lager.Logger
}

func New(name string, db sql.DB, logger lager.Logger) Database {
	return database{
		name:   name,
		db:     db,
		logger: logger,
	}
}

func (d database) RevokePrivileges() error {
	d.logger.Info(fmt.Sprintf("Revoking priviledges to db '%s'", d.name))

	result, err := d.db.Exec(revokeQuery, d.name)
	if err != nil {
		return fmt.Errorf("Updating db '%s' to revoke priviledges: %s", d.name, err.Error())
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("Updating db '%s' to revoke priviledges: Getting rows affected: %s", d.name, err.Error())
	}

	d.logger.Info(fmt.Sprintf("Updating db '%s' to revoke priviledges: Rows affected: %s", d.name, rowsAffected))

	_, err = d.db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		return fmt.Errorf("Flushing privileges: %s", err.Error())
	}

	return nil
}

func (d database) GrantPrivileges() error {
	d.logger.Info(fmt.Sprintf("Granting priviledges to db '%s'", d.name))

	result, err := d.db.Exec(grantQuery, d.name)
	if err != nil {
		return fmt.Errorf("Updating db '%s' to grant priviledges: %s", d.name, err.Error())
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("Updating db '%s' to grant priviledges: Getting rows affected: %s", d.name, err.Error())
	}

	d.logger.Info(fmt.Sprintf("Updating db '%s' to grant priviledges: Rows affected: %s", d.name, rowsAffected))

	_, err = d.db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		return fmt.Errorf("Flushing privileges: %s", err.Error())
	}

	return nil
}

/*
#
      # In order to change privileges immediately, we must do two things:
      # 1) Flush the privileges
      # 2) Kill any and all active connections
      #
      def reset_active_privileges(database)
        connection.execute('FLUSH PRIVILEGES')

        processes = connection.select('SHOW PROCESSLIST')
        processes.each do |process|
          id, db, user = process.values_at('Id', 'db', 'User')

          if db == database && user != 'root'
            begin
              connection.execute("KILL CONNECTION #{id}")
            rescue ActiveRecord::StatementInvalid => e
              raise unless e.message =~ /Unknown thread id/
            end
          end
        end
      end
*/
// ResetActivePrivileges flushes the privileges and kills all active connections to this database.
// New connections will get the new privileges.
func (d database) ResetActivePrivileges() error {
	//
	//    rows, err := d.db.Query("SELECT ID FROM INFORMATION_SCHEMA.PROCESSLIST WHERE DB = ? AND USER <> ?", d.name, "root")
	//    if err != nil {
	//        return fmt.Errorf("Getting list of open connections to database '%s': %s", d.name, err.Error())
	//    }
	//    defer rows.Close()
	//    for rows.Next() {
	//        var connectionID string
	//        if err := rows.Scan(&connectionID); err != nil {
	//            return fmt.Errorf("Scanning open connections to database '%s': %s", d.name, err.Error())
	//        }
	//
	//        _, err := d.db.Exec("KILL CONNECTION ?", connectionID) //TODO: result?
	//        if err != nil {
	//            return fmt.Errorf("Killing connection: %s", err.Error())
	//        }
	//    }
	//    if err := rows.Err(); err != nil {
	//        return fmt.Errorf("Reading open connections to database '%s': %s", d.name, err.Error())
	//    }

	return nil
}
