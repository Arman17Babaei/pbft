package monitoring

import (
	"errors"
	"fmt"
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

func ServeMetrics(config *configs.Config) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	address := fmt.Sprintf("%s:%d", config.Address.Host, config.Address.Port+2000)
	server := &http.Server{
		Addr:    address,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("Metrics server error: %v\n", err)
		}
	}()
}
