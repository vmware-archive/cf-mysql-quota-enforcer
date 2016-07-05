package enforcer_test

import (
	"errors"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/clock/clockfakes"
	enforcerPkg "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/enforcer"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/enforcer/enforcerfakes"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
)

var _ = Describe("Runner", func() {

	var (
		enforcer *enforcerfakes.FakeEnforcer
		clock    *clockfakes.FakeClock
		logger   *lagertest.TestLogger
		runner   ifrit.Runner
		signals  chan os.Signal
		ready    chan struct{}
	)

	BeforeEach(func() {
		enforcer = &enforcerfakes.FakeEnforcer{}
		clock = &clockfakes.FakeClock{}
		logger = lagertest.NewTestLogger("Runner test")
		runner = enforcerPkg.NewRunner(enforcer, clock, logger)

		signals = make(chan os.Signal, 1)
		ready = make(chan struct{})
		go func() {
			<-ready
			signals <- os.Interrupt
		}()

		clock.AfterStub = func(d time.Duration) <-chan time.Time {
			return time.After(1 * time.Millisecond)
		}
	})

	It("runs the enforcer", func() {
		runner.Run(signals, ready)
		Expect(enforcer.EnforceOnceCallCount()).To(BeNumerically(">", 0))
	})

	It("sleeps between calls to the enforcer", func() {
		runner.Run(signals, ready)
		Expect(clock.AfterCallCount()).To(BeNumerically(">", 0))
		for _, sleep := range clock.Invocations()["After"] {
			Expect(sleep[0]).To(Equal(1 * time.Second))
		}
	})

	Context("when the enforcer errors", func() {
		It("logs the error", func() {
			enforcer.EnforceOnceStub = func() error {
				return errors.New("failed to uphold the law")
			}
			runner.Run(signals, ready)
			Expect(logger.TestSink.LogMessages()).To(
				ContainElement(ContainSubstring("failed to uphold the law")))
		})
	})

})
