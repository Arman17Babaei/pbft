package main

import (
	"github.com/Arman17Babaei/pbft/client"
	"github.com/Arman17Babaei/pbft/config"
	log "github.com/sirupsen/logrus"
)


func main() {
	var clientConfig client.Config
	err := config.LoadConfig(&clientConfig, "client")
	if err != nil {
		log.WithError(err).Fatal("could not load config")
	}

	c := client.NewClient(&clientConfig)
	httpServer := client.NewHttpServer(&clientConfig, c)
	go c.Serve()
	go httpServer.Serve()

	select {}
}
