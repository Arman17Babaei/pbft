package main

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/alecthomas/kong"

	pbft "github.com/Arman17Babaei/pbft/consensus"
)

type CLI struct {
	InitValue       int      `help:"Initial value." default:"0" short:"i"`
	MyHTTPPort      int      `help:"HTTP port for the application." required:"true" short:"p"`
	MyRaftPort      int      `help:"Raft port for the application." required:"true" short:"m"`
	OthersRaftPorts []int    `help:"List of other Raft ports." required:"true" short:"o"`
}

func makeHandlers(cm *pbft.ConsensusManager) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "node.html")
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		value := cm.Get()
		fmt.Fprint(w, value)
	})

	http.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}
		v, err := strconv.Atoi(r.FormValue("value"))
		if err != nil {
			http.Error(w, "Invalid value", http.StatusBadRequest)
			return
		}
		cm.Add(v)
		fmt.Fprint(w, "committed")
	})

	http.HandleFunc("/sub", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}
		v, err := strconv.Atoi(r.FormValue("value"))
		if err != nil {
			http.Error(w, "Invalid value", http.StatusBadRequest)
			return
		}
		cm.Sub(v)
		fmt.Fprint(w, "committed")
	})

	http.HandleFunc("/mul", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}
		v, err := strconv.Atoi(r.FormValue("value"))
		if err != nil {
			http.Error(w, "Invalid value", http.StatusBadRequest)
			return
		}
		cm.Mul(v)
		fmt.Fprint(w, "committed")
	})

	http.HandleFunc("/get_status", func(w http.ResponseWriter, r *http.Request) {
		status := cm.GetStatus()
		fmt.Fprint(w, status)
	})
}

func main() {
	var cli CLI
	kong.Parse(&cli)

	fmt.Printf("InitValue: %d\n", cli.InitValue)
	fmt.Printf("MyHTTPPort: %d\n", cli.MyHTTPPort)
	fmt.Printf("MyRaftPort: %d\n", cli.MyRaftPort)
	fmt.Printf("OthersRaftPorts: %v\n", cli.OthersRaftPorts)

	consensusManager := pbft.NewConsensusManager(cli.InitValue, cli.MyRaftPort, cli.OthersRaftPorts)
	consensusManager.Start()

	makeHandlers(consensusManager)

	httpPort := fmt.Sprintf(":%d", cli.MyHTTPPort)
	fmt.Printf("Starting HTTP server on port %s...\n", httpPort)
	if err := http.ListenAndServe(httpPort, nil); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}
