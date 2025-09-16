package monitoring

import (
	"fmt"
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

func ServeMetrics(config *configs.Config) {
	http.Handle("/metrics", promhttp.Handler())
	address := fmt.Sprintf("%s:%d", config.Address.Host, config.Address.Port+2000)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		return
	}
}
