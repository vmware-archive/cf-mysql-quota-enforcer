package enforcer

import (
	"os"
	"time"

	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/clock"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
)

type runner struct {
	enforcer Enforcer
	clock    clock.Clock
	logger   lager.Logger
}

func NewRunner(enforcer Enforcer, clock clock.Clock, logger lager.Logger) ifrit.Runner {
	return &runner{
		enforcer: enforcer,
		clock:    clock,
		logger:   logger,
	}
}

func (r runner) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)
	for {
		err := r.enforcer.EnforceOnce()
		if err != nil {
			r.logger.Error("Enforcing Failed", err)
		}
		select {
		case <-signals:
			return nil
		case <-r.clock.After(1 * time.Second):
		}
	}
}
