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

var ResponseTimeSummary = promauto.NewSummaryVec(
	prometheus.SummaryOpts{
		Name:       "node_response_time",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	},
	[]string{"node", "action"},
)

var InProgressRequestsGauge = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "in_progress_requests",
	},
	[]string{"node"},
)

var MessageStatusCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "message_status",
	},
	[]string{"from", "to", "action", "status"},
)

var ClientRequestStatusCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "client_request_status",
	},
	[]string{"status"},
)

var ClientRequestLatencySummary = promauto.NewSummaryVec(
	prometheus.SummaryOpts{
		Name:       "client_request_latency",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	},
	[]string{"node"},
)
