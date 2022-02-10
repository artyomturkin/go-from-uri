package watermill

import (
	fromuri "github.com/artyomturkin/go-from-uri"
	kafka2 "github.com/artyomturkin/go-from-uri/kafka"
	sqlUri "github.com/artyomturkin/go-from-uri/sql"
	"net/url"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/qairjar/watermill-elastic-plugin"
)

// NewPublisher build watermill publisher from provided url.
func NewPublisher(connection string, logger watermill.LoggerAdapter) (message.Publisher, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}
	var pub message.Publisher
	switch u.Scheme {
	case "mysql", "oracle", "postgres":
		pub, err = sqlUri.NewPublisher(connection, logger)
	case "elastic":
		elasticPub := &elasticplugin.Publisher{ElasticURL: connection}
		pub, err = elasticPub.NewPublisher(nil, logger)
	case "kafka", "kafkas":
		pub, err = kafka2.NewWatermillPublisher(connection, logger)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fromuri.ErrUnsupportedScheme
	}

	return pub, err
}

// NewSubscriber build watermill subscriber from provided url.
func NewSubscriber(connection string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}

	var c message.Subscriber
	switch u.Scheme {
	case "mysql", "oracle", "postgres":
		c, err = sqlUri.NewSubscriber(connection, logger)
	case "elastic":
	case "kafka", "kafkas":
		c, err = kafka2.NewWatermillSubscriber(connection, logger)
	default:
		return nil, fromuri.ErrUnsupportedScheme
	}



	return c, err
}
