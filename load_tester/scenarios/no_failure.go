package scenarios

import (
	"github.com/Arman17Babaei/pbft/load_tester/config"
	"github.com/Arman17Babaei/pbft/pbft"
)

type NoFailure struct {
}

func (n *NoFailure) PrepareScenario(_ *config.Config, _ *pbft.Config) {
	// Scenario needs nothing
}

func (n *NoFailure) Run(_ <-chan any) {
	// No need to touch the nodes
}
