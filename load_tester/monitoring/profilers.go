package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var RequestCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "load_test_request_count",
	},
	[]string{"client", "status"},
)
