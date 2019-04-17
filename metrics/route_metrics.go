package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type RouteMetrics struct {
	Buffer        *BufferMetrics
	InMetrics     prometheus.Counter
	OutMetrics    prometheus.Counter
	Errors        *prometheus.CounterVec
	WriteDuration *prometheus.HistogramVec
}

func NewRouteMetrics(namespace, id, routeType string, additionnalLabels prometheus.Labels) *RouteMetrics {
	if additionnalLabels == nil {
		additionnalLabels = prometheus.Labels{}
	}
	additionnalLabels["id"] = id
	sm := RouteMetrics{}
	sm.Buffer = NewBufferMetrics(fmt.Sprintf("%s_%s", namespace, SpoolSystem), id, additionnalLabels)
	sm.InMetrics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   namespace,
		Subsystem:   SpoolSystem,
		Name:        "incoming_metrics_total",
		Help:        "total number of incoming metrics in the route",
		ConstLabels: additionnalLabels,
	})
	sm.WriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   namespace,
		Subsystem:   SpoolSystem,
		Name:        "write_duration_seconds",
		ConstLabels: additionnalLabels,
	}, []string{"destination"})
	return &sm
}
