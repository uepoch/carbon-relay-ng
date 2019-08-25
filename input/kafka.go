package input

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/graphite-ng/carbon-relay-ng/encoding"
	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/snappy"
	"go.uber.org/zap"
)

type KafkaNew struct {
	BaseInput

	workers int
	reader  *kafka.Reader
	ctx     context.Context
	closed  chan bool
	logger  *zap.Logger
}

func NewKafkaNew(id string, c *kafka.ReaderConfig, h encoding.FormatAdapter, workers int) (*KafkaNew, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	logger := zap.L()
	logger = logger.With(zap.String("id", id), zap.String("kafka_topic", c.Topic), zap.String("kafka_group_id", c.GroupID))

	stdLogger := zap.NewStdLog(logger)
	c.Logger = stdLogger
	c.ErrorLogger = stdLogger

	for _, b := range c.Brokers {
		if _, err := kafka.Dial("tcp", b); err != nil {
			return nil, fmt.Errorf("can't connect to broker %s: %s", b, err)
		}
	}

	reader := kafka.NewReader(*c)

	return &KafkaNew{
		BaseInput: BaseInput{handler: h, name: fmt.Sprintf("kafka[topic=\"%s\",cg=\"%s\"]", c.Topic, c.GroupID)},
		reader:    reader,
		ctx:       context.Background(),
		closed:    make(chan bool),
		logger:    logger,
		workers:   workers,
	}, nil
}

func (kafka *KafkaNew) Name() string {
	return "kafka"
}

func (k *KafkaNew) consume(buf []byte) {
	err := k.handle(buf)
	if err != nil {
		k.logger.Debug("badly formatted metric", zap.ByteString("metric", buf))
	}
}

func (k *KafkaNew) run() {
	r := k.reader
	for i := 0; i < k.workers; i++ {
		go func() {
			for {
				msg, err := r.ReadMessage(k.ctx)
				if err != nil {
					k.logger.Error("error reading message from Kafka", zap.Error(err))
				}
				k.consume(msg.Value)
			}
		}()
	}
}

func (k *KafkaNew) Start(d Dispatcher) error {
	k.Dispatcher = d

	ctx, _ := context.WithTimeout(k.ctx, 2*time.Second)

	msg, err := k.reader.ReadMessage(ctx)
	if err != nil {
		return err
	}

	k.consume(msg.Value)

	go k.run()
	return nil
}

func (k *KafkaNew) close() error {
	err := k.reader.Close()
	if err != nil {
		k.logger.Error("kafka input closed with errors.", zap.Error(err))
	} else {
		k.logger.Info("kafka input closed correctly.")
	}
	return err
}

func (k *KafkaNew) Stop() error {
	return k.close()
}
