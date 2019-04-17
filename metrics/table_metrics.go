package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const tableNamespace = "table"

const TableErrorTypeOutOfOrder = "out_of_order"
const TableErrorTypeInvalid = "invalid"
const TableErrorTypeBlacklist = "blacklist"
const TableErrorTypeUnroutable = "unroutable"

type TableMetrics struct {
	RoutingDuration prometheus.Histogram
	In              prometheus.Counter
	Unrouted        *prometheus.CounterVec
}

func NewTableMetrics() *TableMetrics {
	namespace := tableNamespace
	tm := TableMetrics{}

	tm.RoutingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "routing_duration_seconds",
		Help:      "time spent routing metrics",
	})
	tm.In = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "incoming_metrics_total",
		Help:      "total number of incoming metrics",
	})
	tm.Unrouted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "unrouted_metrics_total",
		Help:      "Total number of metrics not routed for `reason`",
	}, []string{"reason"})

	return &tm
}
