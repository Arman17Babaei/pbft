package main

import (
	// Standard library imports

	"github.com/Arman17Babaei/pbft/pbft/leader_election"
	// Third party imports
	"github.com/alecthomas/kong"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	// Local imports
	"github.com/Arman17Babaei/pbft/config"
	"github.com/Arman17Babaei/pbft/pbft"
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	"github.com/Arman17Babaei/pbft/pbft/view_changer"
	pb "github.com/Arman17Babaei/pbft/proto"
)

type CLI struct {
	Id      string `help:"Alternative ID for what's in pbft config.'"`
	Cluster bool   `help:"Run the all nodes of pbft config."`
}

func main() {
	log.SetLevel(log.InfoLevel)
	var cli CLI
	kong.Parse(&cli)

	pbftConfig, err := loadConfiguration(&cli)
	if err != nil {
		log.WithError(err).Fatal("could not load config")
	}

	if cli.Cluster {
		runClusterMode(pbftConfig)
	} else {
		runSingleNode(pbftConfig)
	}

	select {} // Block forever
}

func loadConfiguration(cli *CLI) (configs.Config, error) {
	var pbftConfig configs.Config
	err := config.LoadConfig(&pbftConfig, "pbft")
	if err != nil {
		return pbftConfig, err
	}

	if !cli.Cluster && len(cli.Id) > 0 {
		if address, ok := pbftConfig.PeersAddress[cli.Id]; ok {
			pbftConfig.Address = address
			pbftConfig.Id = cli.Id
			log.WithField("id", pbftConfig.Id).Info("Using specified node ID")
		} else {
			log.WithField("id", cli.Id).Error("Address not specified in config, ignoring ID")
		}
	}

	return pbftConfig, nil
}

func runClusterMode(config configs.Config) {
	log.Info("Running in cluster mode")
	for id, address := range config.PeersAddress {
		configCopy := config
		configCopy.Id = id
		configCopy.Address = address
		startNode(configCopy)
	}
}

func runSingleNode(config configs.Config) {
	log.Info("Running in single node mode")
	startNode(config)
}

func startNode(config configs.Config) {
	channels := createChannels()
	store := pbft.NewStore(&config)

	pbftSender := pbft.NewSender(&config)
	viewChangeSender := view_changer.NewSender(&config)

	node := pbft.NewNode(&config, pbftSender, channels.input, channels.request,
		channels.enable, channels.disable, store)

	leaderElection := leader_election.NewRoundRobinLeaderElection(&config)
	viewChanger := setupViewChanger(&config, store, viewChangeSender, node, leaderElection)
	node.SetViewChanger(viewChanger)

	startServices(config, channels, node, viewChanger)
}

type nodeChannels struct {
	input      chan proto.Message
	viewChange chan proto.Message
	request    chan *pb.ClientRequest
	enable     chan any
	disable    chan any
}

func createChannels() nodeChannels {
	return nodeChannels{
		input:      make(chan proto.Message),
		viewChange: make(chan proto.Message),
		request:    make(chan *pb.ClientRequest, 50),
		enable:     make(chan any),
		disable:    make(chan any),
	}
}

func setupViewChanger(config *configs.Config, store *pbft.Store,
	sender view_changer.ISender, node *pbft.Node, leaderElection view_changer.LeaderElection,
) *view_changer.PbftViewChange {
	viewChanger := view_changer.NewPbftViewChange(config, store, sender, leaderElection)
	viewChanger.SetNode(node)
	return viewChanger
}

func startServices(config configs.Config, channels nodeChannels,
	node *pbft.Node, viewChanger *view_changer.PbftViewChange,
) {
	service := pbft.NewService(channels.input, channels.request,
		channels.enable, channels.disable, &config)
	viewChangeService := view_changer.NewService(channels.viewChange, &config)

	go node.Run()
	go viewChanger.Run(channels.viewChange)
	go service.Serve()
	go viewChangeService.Serve()
	go monitoring.ServeMetrics(&config)
}
