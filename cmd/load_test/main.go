package main

import (
	"github.com/Arman17Babaei/pbft/config"
	"github.com/Arman17Babaei/pbft/load_tester"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.ErrorLevel)
	var loadTesterConfig load_tester.Config
	err := config.LoadConfig(&loadTesterConfig, "load_tester")
	if err != nil {
		log.WithError(err).Fatal("could not load config")
	}

	loadTester := load_tester.NewLoadTest(&loadTesterConfig)
	loadTester.Run()
}
