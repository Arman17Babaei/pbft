package monitoring

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

func ServeMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	address := fmt.Sprintf("%s:%d", "0.0.0.0", 7201)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		return
	}
}
