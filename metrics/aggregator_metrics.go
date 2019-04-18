package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const aggregatorNamespace = "aggregator"

const cacheSystem = "cache"

type CacheMetrics struct {
	Hits   prometheus.Counter
	Misses prometheus.Counter
}

func NewCacheMetrics(namespace string, labels prometheus.Labels) *CacheMetrics {
	cm := CacheMetrics{}

	cVec := promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace:   namespace,
		Subsystem:   cacheSystem,
		Name:        "requests_total",
		Help:        "Total number of requests processed by cache",
		ConstLabels: labels,
	}, []string{"status"})
	cm.Hits = cVec.WithLabelValues("hit")
	cm.Misses = cVec.WithLabelValues("miss")

	return &cm
}

type AggregatorMetrics struct {
	Cache            *CacheMetrics
	RoutingDuration  prometheus.Histogram
	In               prometheus.Counter
	Unrouted         *prometheus.CounterVec
	LowestTimestamp  prometheus.Gauge
	HighestTimestamp prometheus.Gauge
}

func NewAggregatorMetrics(id string, labels prometheus.Labels) *AggregatorMetrics {
	namespace := aggregatorNamespace
	cm := AggregatorMetrics{}

	if labels == nil {
		labels = prometheus.Labels{}
	}
	labels["id"] = id

	cm.Cache = NewCacheMetrics(namespace, labels)

	tsVec := promauto.NewGaugeVec(prometheus.CounterOpts{
		Namespace:   namespace,
		Name:        "timestamp_value",
		Help:        "Lowest and Highest Timestamp registered",
		ConstLabels: labels,
	}, []string{"type"})

	cm.HighestTimestamp = tsVec.WithLabelValues("highest")
	cm.LowestTimestamp = tsVec.WithLabelValues("lowest")

	cm.In = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   namespace,
		Name:        "incoming_metrics_total",
		Help:        "total number of incoming metrics",
		ConstLabels: labels,
	})
	cm.Unrouted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace:   namespace,
		Name:        "unrouted_metrics_total",
		Help:        "Total number of metrics not routed for `reason`",
		ConstLabels: labels,
	}, []string{"reason"})

	return &cm
}

func (am *AggregatorMetrics) ObserveTimestamp(ts uint32) {
	am.

}
