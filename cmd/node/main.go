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
	Id      string `help:"Alternative ID for what's in pbft config.'"`
	Cluster bool   `help:"Run the all nodes of pbft config."`
}

func main() {
	log.SetLevel(log.WarnLevel)
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
		if len(cli.Id) > 0 {
			if _, ok := pbftConfig.PeersAddress[cli.Id]; !ok {
				log.WithField("id", cli.Id).Error("Address not specified in config, ignoring ID")
			} else {
				pbftConfig.Address = pbftConfig.PeersAddress[cli.Id]
				pbftConfig.Id = cli.Id
				log.WithField("id", pbftConfig.Id).Info("Replaced config ID")
			}
		}
		startNode(pbftConfig)
	}

	// Wait forever
	select {}
}

func startNode(config pbft.Config) {
	inputCh := make(chan proto.Message)
	requestCh := make(chan *pb.ClientRequest)
	enableCh := make(chan any)
	disableCh := make(chan any)
	service := pbft.NewService(inputCh, requestCh, enableCh, disableCh, &config)
	sender := pbft.NewSender(&config)
	node := pbft.NewNode(&config, sender, inputCh, requestCh, enableCh, disableCh)

	go node.Run()
	go service.Serve()
}
