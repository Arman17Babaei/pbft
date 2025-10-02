package main

import (
	"os"
	"time"

	"github.com/Arman17Babaei/pbft/config"
	"github.com/Arman17Babaei/pbft/load_tester"
	"github.com/Arman17Babaei/pbft/load_tester/configs"
	"github.com/Arman17Babaei/pbft/load_tester/monitoring"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.ErrorLevel)
	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.TimeOnly,
	})
	var loadTesterConfig configs.Config
	err := config.LoadConfig(&loadTesterConfig, "load_tester")
	if err != nil {
		log.WithError(err).Fatal("could not load config")
	}

	loadTester := load_tester.NewLoadTest(&loadTesterConfig)
	go loadTester.Run()
	monitoring.ServeMetrics()
}
