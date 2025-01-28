package scenarios

import (
	"github.com/Arman17Babaei/pbft/load_tester/config"
	"github.com/Arman17Babaei/pbft/pbft"
)

type Scenario interface {
	PrepareScenario(loadTestConfig *config.Config, pbftConfig *pbft.Config)
	Run(stopCh <-chan any)
}

var Scenarios = map[string]Scenario{
	"no_failure":       &NoFailure{},
	"periodic_failure": &PeriodicFailure{},
}
