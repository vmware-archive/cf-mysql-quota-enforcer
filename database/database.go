package database

type Database interface {
    GrantPrivileges() error
    RevokePrivileges() error
    ResetActivePrivileges() error
}

type database struct{}

func New() Database {
	return database{}
}

func (d database) RevokePrivileges() error {
	return nil
}

func (d database) GrantPrivileges() error {
	return nil
}

func (d database) ResetActivePrivileges() error {
	return nil
}
