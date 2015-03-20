package database

import (
    "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/sql"
    "fmt"
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

type database struct{
    name string
    db sql.DB
}

func New(name string, db sql.DB) Database {
	return database{
        name: name,
        db: db,
    }
}

func (d database) RevokePrivileges() error {
    result, err := d.db.Exec(revokeQuery, d.name)
    if err != nil {
        return fmt.Errorf("Updating db '%s' to revoke priviledges: %s", d.name, err.Error())
    }

    //TODO: do we even care about rows affected? Is db name the PK or will there be multiple rows, one per user with access?
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return fmt.Errorf("Updating db '%s' to revoke priviledges: Getting rows affected: %s", d.name, err.Error())
    }

    if rowsAffected != 1 {
        return fmt.Errorf("Updating db '%s' to revoke priviledges: Affected %d rows, expected 1", d.name, rowsAffected)
    }

    return nil
}

func (d database) GrantPrivileges() error {
    result, err := d.db.Exec(grantQuery, d.name)
    if err != nil {
        return fmt.Errorf("Updating db '%s' to grant priviledges: %s", d.name, err.Error())
    }

    //TODO: do we even care about rows affected? Is db name the PK or will there be multiple rows, one per user with access?
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return fmt.Errorf("Updating db '%s' to grant priviledges: Getting rows affected: %s", d.name, err.Error())
    }

    if rowsAffected != 1 {
        return fmt.Errorf("Updating db '%s' to grant priviledges: Affected %d rows, expected 1", d.name, rowsAffected)
    }

    return nil
}

func (d database) ResetActivePrivileges() error {
	return nil
}
