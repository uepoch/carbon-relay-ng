package aggregator

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const prometheusNamespace = "aggregator"

var counterTooOldMetrics = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: prometheusNamespace,
	Name:      "metrics_too_old_total",
	Help:      "The total number of metrics that couldn't be aggregated because of their age",
}, []string{"prefix"})

var rangeTracker *RangeTracker

func InitMetrics() {
	rangeTracker = NewRangeTracker()
}

type RangeTracker struct {
	sync.Mutex
	min  uint32
	max  uint32
	minG prometheus.Gauge
	maxG prometheus.Gauge
}

func NewRangeTracker() *RangeTracker {
	m := &RangeTracker{
		min: math.MaxUint32,
		minG: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: prometheusNamespace,
			Name:      "timestamp_received_min",
			Help:      "The oldest timestamp that was received by the aggregator",
		}),
		maxG: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: prometheusNamespace,
			Name:      "timestamp_received_max",
			Help:      "The newest timestamp that was received by the aggregator",
		}),
	}
	go m.Run()
	return m
}

func (m *RangeTracker) Run() {
	for now := range time.Tick(time.Second) {
		m.Lock()
		min := m.min
		max := m.max
		m.min = math.MaxUint32
		m.max = 0
		m.Unlock()

		// if we have not seen any value yet, just report "in sync"
		if max == 0 {
			min = uint32(now.Unix())
			max = min
		}

		m.minG.Set(float64(min))
		m.maxG.Set(float64(max))
	}
}

func (m *RangeTracker) Sample(ts uint32) {
	m.Lock()
	if ts > m.max {
		m.max = ts
	}
	if ts < m.min {
		m.min = ts
	}
	m.Unlock()
}
