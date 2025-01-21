package main

import (
	"github.com/Arman17Babaei/pbft/config"
	"github.com/Arman17Babaei/pbft/pbft"
	pb "github.com/Arman17Babaei/pbft/proto"
	"github.com/alecthomas/kong"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type CLI struct {
	Cluster bool `help:"Run the all nodes of pbft config."`
}

func main() {
	log.SetLevel(log.TraceLevel)
	var cli CLI
	kong.Parse(&cli)

	var pbftConfig pbft.Config
	err := config.LoadConfig(&pbftConfig, "pbft")
	if err != nil {
		log.WithError(err).Fatal("could not load config")
	}

	if cli.Cluster {
		log.Info("Running cluster")
		for id, address := range pbftConfig.PeersAddress {
			log.WithField("node", id).Info("node configuration")
			configCopy := pbftConfig
			configCopy.Id = id
			configCopy.Address = address
			startNode(configCopy)
		}
	} else {
		log.Info("Running single node")
		startNode(pbftConfig)
	}

	// Wait forever
	select {}
}

func startNode(config pbft.Config) {
	inputCh := make(chan proto.Message)
	requestCh := make(chan *pb.ClientRequest)
	service := pbft.NewService(inputCh, requestCh, &config)
	sender := pbft.NewSender(&config)
	node := pbft.NewNode(&config, sender, inputCh, requestCh)

	go node.Run()
	go service.Serve()
}
