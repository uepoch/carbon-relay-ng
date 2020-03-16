package route

import (
	"context"
	"fmt"
	"github.com/graphite-ng/carbon-relay-ng/destination"
	"time"

	"github.com/graphite-ng/carbon-relay-ng/encoding"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/metrics"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Kafka struct {
	baseRoute
	router *RoutingMutator
	Writer *kafka.Writer

	wrCh   chan encoding.Datapoint
	spool  *destination.Spool
	ctx    context.Context
	cancel func()
	fetch  func() encoding.Datapoint
}

func NewKafkaRoute(key, prefix, sub, regex string, config kafka.WriterConfig, routingMutator *RoutingMutator, spool bool, spoolDir string) (*Kafka, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %s", err)
	}

	m, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	k := Kafka{
		baseRoute: *newBaseRoute(key, "kafka", *m),
		router:    routingMutator,
		Writer:    kafka.NewWriter(config),
		wrCh:      make(chan encoding.Datapoint, config.QueueCapacity),
		ctx:       ctx,
		cancel:    cancel,
	}

	if spool {
		k.spool = destination.NewSpool(key, spoolDir, config.QueueCapacity, 0, 0, 0, 0, 500*time.Millisecond)
	}

	if err := metrics.RegisterKafkaMetrics(key, k.Writer); err != nil {
		return nil, fmt.Errorf("can't register kafka metrics: %s", err)
	}
	k.rm = metrics.NewRouteMetrics(key, "kafka", nil)
	k.logger = k.logger.With(zap.String("kafka_topic", config.Topic))

	return &k, nil
}

func (k *Kafka) Shutdown() error {
	k.logger.Info("shutting down kafka route")
	k.cancel()
	k.spool.Close()
	return k.Writer.Close()
}

func (k *Kafka) write(dp encoding.Datapoint) {
	key := []byte(dp.Name)
	if newKey, ok := k.router.HandleBuf(key); ok {
		key = newKey
	}


	err := k.Writer.WriteMessages(k.ctx, kafka.Message{Key: key, Value: []byte(dp.String()), Headers: getKafkaHeader(dp.Tags)})

	if err != nil {
		k.logger.Error("error writing to kafka", zap.Error(err))
		k.rm.Errors.WithLabelValues(err.Error())
	} else {
		k.rm.OutMetrics.Inc()
	}
}
func getKafkaHeader(tags map[string]string) []kafka.Header {
	headers := make([]kafka.Header, len(tags))
	i := 0
	for key, value := range tags {
		header := kafka.Header{Key: key, Value: []byte(value)}
		headers[i] = header
		i++
	}
	return headers
}

func (k *Kafka) run() {
	var toUnspool chan encoding.Datapoint
	if k.spool != nil {
		toUnspool = k.spool.Out
	}
	for {
		var dp encoding.Datapoint
		select {
		case dp = <-k.wrCh:
			k.rm.Buffer.BufferedMetrics.Dec()
		case dp = <-toUnspool:
		case <-k.ctx.Done():
			k.logger.Info("stopped kafka producer")
			return
		}
		k.write(dp)
	}
}

func (k *Kafka) Dispatch(dp encoding.Datapoint) {
	k.rm.InMetrics.Inc()
	t := time.Now()
	k.writeOrSpool(dp)
	k.rm.Buffer.WriteDuration.Observe(time.Since(t).Seconds())
}

func (k *Kafka) writeNoSpool(dp encoding.Datapoint) {
	k.wrCh <- dp
}

func (k *Kafka) writeOrSpool(dp encoding.Datapoint) {
	select {
	case k.wrCh <- dp:
		k.rm.Buffer.BufferedMetrics.Inc()
	default:
		select {
		case k.wrCh <- dp:
			k.rm.Buffer.BufferedMetrics.Inc()
		case k.spool.InRT <- dp:
		}
	}
}

func (k *Kafka) Snapshot() Snapshot {
	return makeSnapshot(&k.baseRoute)
}
