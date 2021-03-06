// +build windows

package jobsupervisor

import (
	"os"
	"time"

	"github.com/pivotal-golang/clock"

	boshhandler "github.com/cloudfoundry/bosh-agent/handler"
	boshmonit "github.com/cloudfoundry/bosh-agent/jobsupervisor/monit"
	boshplatform "github.com/cloudfoundry/bosh-agent/platform"
	boshdir "github.com/cloudfoundry/bosh-agent/settings/directories"
	bosherr "github.com/cloudfoundry/bosh-utils/errors"
	boshlog "github.com/cloudfoundry/bosh-utils/logger"
)

const jobSupervisorListenPort = 2825

type Provider struct {
	supervisors map[string]JobSupervisor
}

func NewProvider(
	platform boshplatform.Platform,
	client boshmonit.Client,
	logger boshlog.Logger,
	dirProvider boshdir.Provider,
	handler boshhandler.Handler,
) (p Provider) {
	fs := platform.GetFs()
	runner := platform.GetRunner()
	timeService := clock.NewClock()
	monitJobSupervisor := NewMonitJobSupervisor(
		fs,
		runner,
		client,
		logger,
		dirProvider,
		jobSupervisorListenPort,
		MonitReloadOptions{
			MaxTries:               3,
			MaxCheckTries:          6,
			DelayBetweenCheckTries: 5 * time.Second,
		},
		timeService,
	)

	network, err := platform.GetDefaultNetwork()
	var machineIP string
	if err != nil {
		machineIP, _ = os.Hostname()
		logger.Debug("providerWindows", "Initializing jobsupervisor.provider_windows: %s, using hostname \"%s\"instead of IP", err, machineIP)
	} else {
		machineIP = network.IP
	}

	p.supervisors = map[string]JobSupervisor{
		"monit":      monitJobSupervisor,
		"dummy":      NewDummyJobSupervisor(),
		"dummy-nats": NewDummyNatsJobSupervisor(handler),
		"windows": NewWindowsJobSupervisor(runner, dirProvider, fs, logger, jobSupervisorListenPort,
			make(chan bool), machineIP),
	}

	return
}

func (p Provider) Get(name string) (supervisor JobSupervisor, err error) {
	supervisor, found := p.supervisors[name]
	if !found {
		err = bosherr.Errorf("JobSupervisor %s could not be found", name)
	}
	return
}
