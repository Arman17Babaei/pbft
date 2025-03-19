package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var MessageCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "message_count",
	},
	[]string{"from", "to", "type"},
)

var ExecutedRequestsGauge = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "executed_requests",
	},
	[]string{"node"},
)
