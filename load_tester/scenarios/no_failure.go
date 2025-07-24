package scenarios

import (
	"github.com/Arman17Babaei/pbft/load_tester/configs"
	config2 "github.com/Arman17Babaei/pbft/pbft/configs"
)

type NoFailure struct {
}

func (n *NoFailure) PrepareScenario(_ *configs.Config, _ *config2.Config) {
	// Scenario needs nothing
}

func (n *NoFailure) Run(_ <-chan any) {
	// No need to touch the nodes
}
