package client

import (
	"fmt"
	"net/http"
	"time"

	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
)

type HttpServer struct {
	config *Config
	client *Client
}

func NewHttpServer(config *Config, client *Client) *HttpServer {
	return &HttpServer{
		config: config,
		client: client,
	}
}

func (h *HttpServer) Serve() {
	if h.config.HttpAddress == nil {
		log.Info("not starting http server as address is nil")
		return
	}

	h.MakeHandlers()

	hostAddress := fmt.Sprintf("%s:%d", h.config.HttpAddress.Host, h.config.HttpAddress.Port)
	log.WithField("address", hostAddress).Info("starting HTTP server on port...")
	if err := http.ListenAndServe(hostAddress, nil); err != nil {
		log.WithError(err).Error("error starting server", err)
	}
}

func (h *HttpServer) MakeHandlers() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "client/node.html")
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		op := &pb.Operation{Type: pb.Operation_GET}
		resultCh := make(chan *pb.OperationResult)
		h.client.SendRequest(op, resultCh)
		timer := time.NewTimer(time.Duration(h.config.HttpTimeoutMs) * time.Millisecond)
		select {
		case result := <-resultCh:
			fmt.Fprint(w, result.Value)
		case <-timer.C:
			log.Error("timeout waiting for response")
			http.Error(w, "timeout", http.StatusRequestTimeout)
		}
	})

	http.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			log.WithField("method", r.Method).Error("invalid method (!= POST)")
			http.Error(w, "invalid method", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			log.WithError(err).Error("failed to parse form")
			http.Error(w, "failed to parse form", http.StatusBadRequest)
			return
		}

		op := &pb.Operation{Type: pb.Operation_ADD, Value: r.FormValue("value")}
		resultCh := make(chan *pb.OperationResult)
		h.client.SendRequest(op, resultCh)
		timer := time.NewTimer(time.Duration(h.config.HttpTimeoutMs) * time.Millisecond)
		select {
		case result := <-resultCh:
			fmt.Fprint(w, result.Value)
		case <-timer.C:
			log.Error("timeout waiting for response")
			http.Error(w, "timeout", http.StatusRequestTimeout)
		}
	})

	http.HandleFunc("/sub", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			log.WithField("method", r.Method).Error("invalid method (!= POST)")
			http.Error(w, "invalid method", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			log.WithError(err).Error("failed to parse form")
			http.Error(w, "failed to parse form", http.StatusBadRequest)
			return
		}

		op := &pb.Operation{Type: pb.Operation_SUB, Value: r.FormValue("value")}
		resultCh := make(chan *pb.OperationResult)
		h.client.SendRequest(op, resultCh)
		timer := time.NewTimer(time.Duration(h.config.HttpTimeoutMs) * time.Millisecond)
		select {
		case result := <-resultCh:
			fmt.Fprint(w, result.Value)
		case <-timer.C:
			log.Error("timeout waiting for response")
			http.Error(w, "timeout", http.StatusRequestTimeout)
		}
	})
}
