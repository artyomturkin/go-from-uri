package kafka

import (
	"net/url"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// NewWatermillPublisher build watermill publisher from provided url.
func NewWatermillPublisher(connection string, logger watermill.LoggerAdapter) (message.Publisher, error) {
	brokers, conf, err := NewSaramaConfig(connection)
	if err != nil {
		return nil, err
	}

	return kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               brokers,
			OverwriteSaramaConfig: conf,
			Marshaler:             kafka.DefaultMarshaler{},
		},
		logger,
	)
}

// NewWatermillSubscriber build watermill subscriber from provided url.
func NewWatermillSubscriber(connection string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	brokers, conf, err := NewSaramaConfig(connection)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}

	c := kafka.SubscriberConfig{
		Brokers:               brokers,
		OverwriteSaramaConfig: conf,
		Unmarshaler:           kafka.DefaultMarshaler{},
	}

	if cg := u.Query().Get("group"); cg != "" {
		c.ConsumerGroup = cg
	}

	return kafka.NewSubscriber(
		c,
		logger,
	)
}
