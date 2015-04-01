package enforcer

import (
	"fmt"

	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
)

type Enforcer interface {
	Enforce() error
}

type enforcer struct {
	violatorRepo, reformerRepo database.Repo
}

func NewEnforcer(violatorRepo, reformerRepo database.Repo) Enforcer {
	return &enforcer{
		violatorRepo: violatorRepo,
		reformerRepo: reformerRepo,
	}
}

func (e enforcer) Enforce() error {
	err := e.revokePrivilegesFromViolators()
	if err != nil {
		return err
	}

	err = e.grantPrivilegesToReformed()
	if err != nil {
		return err
	}

	return nil
}

func (e enforcer) revokePrivilegesFromViolators() error {
	violators, err := e.violatorRepo.All()
	if err != nil {
		return fmt.Errorf("Finding violators: %s", err.Error())
	}

	for _, db := range violators {
		err = db.RevokePrivileges()
		if err != nil {
			return fmt.Errorf("Revoking privileges: %s", err.Error())
		}

		err = db.KillActiveConnections()
		if err != nil {
			return fmt.Errorf("Resetting active privileges: %s", err.Error())
		}
	}
	return nil
}

func (e enforcer) grantPrivilegesToReformed() error {
	reformers, err := e.reformerRepo.All()
	if err != nil {
		return fmt.Errorf("Finding reformers: %s", err.Error())
	}

	for _, db := range reformers {
		err = db.GrantPrivileges()
		if err != nil {
			return fmt.Errorf("Granting privileges: %s", err.Error())
		}

		err = db.KillActiveConnections()
		if err != nil {
			return fmt.Errorf("Resetting active privileges: %s", err.Error())
		}
	}

	return nil
}
