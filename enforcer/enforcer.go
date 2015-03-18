package enforcer

import "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"

type Enforcer interface {
	Enforce() error
}

type impl struct {
	violatorRepo, reformerRepo database.Repo
}

func NewEnforcer(violatorRepo, reformerRepo database.Repo) Enforcer {
	return &impl{
		violatorRepo: violatorRepo,
		reformerRepo: reformerRepo,
	}
}

func (e impl) Enforce() error {
	e.revokePrivilegesFromViolators()
	e.grantPrivilegesToReformed()

	return nil
}

func (e impl) revokePrivilegesFromViolators() error {
	violators, err := e.violatorRepo.All()
	if err != nil {
		return err
	}

	for _, db := range violators {
		db.RevokePrivileges()
		db.ResetActivePrivileges()
	}
	return nil
}

func (e impl) grantPrivilegesToReformed() error {
	reformers, err := e.reformerRepo.All()
	if err != nil {
		return err
	}

	for _, db := range reformers {
		db.GrantPrivileges()
		db.ResetActivePrivileges()
	}

	return nil
}
