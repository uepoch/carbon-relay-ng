package input

import (
	"context"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/segmentio/kafka-go"

	"github.com/graphite-ng/carbon-relay-ng/encoding"
	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/snappy"
	"go.uber.org/zap"
)

type KafkaNew struct {
	BaseInput

	reader *kafka.Reader
	ctx    context.Context
	closed chan bool
	logger *zap.Logger
}

func NewKafkaNew(id string, c *kafka.ReaderConfig, h encoding.FormatAdapter) (*KafkaNew, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	logger := zap.L()
	logger = logger.With(zap.String("id", id), zap.String("topic", c.Topic), zap.String("group_id", c.GroupID))

	stdLogger := zap.NewStdLog(logger)
	c.Logger = stdLogger
	c.ErrorLogger = stdLogger

	reader := kafka.NewReader(*c)

	return &KafkaNew{
		BaseInput: BaseInput{handler: h, name: fmt.Sprintf("kafka[topic=\"%s\",cg=\"%s\"]", c.Topic, c.GroupID)},
		reader:    reader,
		ctx:       context.Background(),
		closed:    make(chan bool),
		logger:    logger,
	}, nil
}

func (kafka *KafkaNew) Name() string {
	return "kafka"
}

func (k *KafkaNew) run() {

}

func (k *KafkaNew) Start(d Dispatcher) error {
	k.Dispatcher = d

	ready := make(chan bool, 0)

	msg, err := k.reader.ReadMessage(k.ctx)
	if err != nil {
		return err
	}

	go k.run()

	go func(c chan bool) {
		for {
			select {
			case <-c:
				return
			default:
			}
			err := k.client.Consume(k.ctx, strings.Fields(k.topic), k)
			if err != nil {
				k.logger.Error("kafka input error Consume method ", zap.Error(err))
			}
			k.ready = make(chan bool, 0)
		}
	}(k.closed)
	<-k.ready // Await till the consumer has been set up
	k.logger.Info("Sarama consumer up and running!...")
	return nil

}
func (k *Kafka) close() {
	err := k.client.Close()
	if err != nil {
		k.logger.Error("kafka input closed with errors.", zap.Error(err))
	} else {
		k.logger.Info("kafka input closed correctly.")
	}
}

func (k *Kafka) Stop() error {
	close(k.closed)
	k.close()
	return nil
}

type Kafka struct {
	BaseInput

	topic      string
	dispatcher Dispatcher
	client     sarama.ConsumerGroup
	ctx        context.Context
	closed     chan bool
	ready      chan bool
	logger     *zap.Logger
}

func (kafka *Kafka) Name() string {
	return "kafka"
}

func (k *Kafka) Start(d Dispatcher) error {
	k.Dispatcher = d

	k.ready = make(chan bool, 0)

	go func() {
		for err := range k.client.Errors() {
			k.logger.Error("kafka input error ", zap.Error(err))
		}
	}()
	go func(c chan bool) {
		for {
			select {
			case <-c:
				return
			default:
			}
			err := k.client.Consume(k.ctx, strings.Fields(k.topic), k)
			if err != nil {
				k.logger.Error("kafka input error Consume method ", zap.Error(err))
			}
			k.ready = make(chan bool, 0)
		}
	}(k.closed)
	<-k.ready // Await till the consumer has been set up
	k.logger.Info("Sarama consumer up and running!...")
	return nil

}
func (k *Kafka) close() {
	err := k.client.Close()
	if err != nil {
		k.logger.Error("kafka input closed with errors.", zap.Error(err))
	} else {
		k.logger.Info("kafka input closed correctly.")
	}
}

func (k *Kafka) Stop() error {
	close(k.closed)
	k.close()
	return nil
}

func NewKafka(id string, brokers []string, topic string, autoOffsetReset int64, consumerGroup string, h encoding.FormatAdapter) *Kafka {
	kafkaConfig := sarama.NewConfig()
	if id != "" {
		kafkaConfig.ClientID = id
	}

	kafkaConfig.Consumer.Return.Errors = true
	kafkaConfig.Consumer.Offsets.Initial = autoOffsetReset
	kafkaConfig.Version = sarama.V2_2_0_0

	logger := zap.L()

	client, err := sarama.NewConsumerGroup(brokers, consumerGroup, kafkaConfig)
	if err != nil {
		logger.Fatal("kafka input init failed", zap.Error(err))
	} else {
		logger.Info("kafka input init correctly")
	}

	return &Kafka{
		BaseInput: BaseInput{handler: h, name: fmt.Sprintf("kafka[topic=%s;cg=%s;id=%s]", topic, consumerGroup, kafkaConfig.ClientID)},
		topic:     topic,
		client:    client,
		ctx:       context.Background(),
		closed:    make(chan bool),
		logger:    logger,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (k *Kafka) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(k.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (k *Kafka) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (k *Kafka) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		k.logger.Debug("metric value:", zap.ByteString("messageValue", message.Value))
		if err := k.handle(message.Value); err != nil {
			k.logger.Debug("invalid message from kafka", zap.ByteString("messageValue", message.Value))
		}
		session.MarkMessage(message, "")
	}
	return nil
}
