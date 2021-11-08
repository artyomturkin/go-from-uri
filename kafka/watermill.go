package kafka

import (
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/artyomturkin/go-from-uri/cassandra"
	"github.com/artyomturkin/go-from-uri/postgres"
    "github.com/qairjar/watermill-scylla-plugin"
	"net/url"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/qairjar/watermill-elastic-plugin"
	"github.com/qairjar/watermill-sql-plugin"
)

// NewWatermillPublisher build watermill publisher from provided url.
func NewWatermillPublisher(connection string, logger watermill.LoggerAdapter) (message.Publisher, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}
	var pub message.Publisher
	switch u.Scheme {
	case "sql":
		db, err := postgres.SetConf(u)
		if err != nil {
			return nil, err
		}
		pub, err = sql.NewPublisher(
			db,
			sql.PublisherConfig{
				SchemaAdapter: sql.DefaultMySQLSchema{},
			},
			logger,
		)
		if err != nil {
			return nil, err
		}
	case "postgres":
		db, err := postgres.SetConf(u)
		if err != nil {
			return nil, err
		}
		posPub := sqlplugin.Publisher{DB: db}
		pub,err = posPub.NewPublisher(nil, logger)
		if err != nil {
			return nil, err
		}
	case "scylla":
		db, err := cassandra.NewScyllaConfig(u)
		if err != nil {
			return nil, err
		}
		pub = &scyllaplugin.Publisher{
			DB: db,
		}
	case "elastic":
		elasticPub := &elasticplugin.Publisher{ElasticURL: u.Host}
		pub,err  = elasticPub.NewPublisher(nil, logger)
		if err != nil {
			return nil, err
		}
	default:
		brokers, conf, err := NewSaramaConfig(u)
		if err != nil {
			return nil, err
		}
		pub, err = kafka.NewPublisher(
			kafka.PublisherConfig{
				Brokers:               brokers,
				OverwriteSaramaConfig: conf,
				Marshaler:             kafka.DefaultMarshaler{},
			},
			logger,
		)
	}

	return pub, err
}

// NewWatermillSubscriber build watermill subscriber from provided url.
func NewWatermillSubscriber(connection string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}

	var c message.Subscriber
	switch u.Scheme {
	case "sql":
		db, err := postgres.SetConf(u)
		if err != nil {
			return nil, err
		}
		sub := sqlplugin.Subscriber{DB: db}
		c, err = sub.NewSubscriber(nil, logger)
		if err != nil {
			return nil, err
		}

	case "postgres":
		db, err := postgres.SetConf(u)
		if err != nil {
			return nil, err
		}
		sub := sqlplugin.Subscriber{DB: db}
		c, err = sub.NewSubscriber(nil, logger)
		if err != nil {
			return nil, err
		}
	case "scylla":
		db, err := cassandra.NewScyllaConfig(u)
		if err != nil {
			return nil, err
		}
		c = &scyllaplugin.Subscriber{
			DB: db,
		}
	case "elastic":
	default:
		brokers, conf, err := NewSaramaConfig(u)
		if err != nil {
			return nil, err
		}
		sc := kafka.SubscriberConfig{
			Brokers:               brokers,
			OverwriteSaramaConfig: conf,
			Unmarshaler:           kafka.DefaultMarshaler{},
		}

		if cg := u.Query().Get("group"); cg != "" {
			sc.ConsumerGroup = cg
		}
		c, _ = kafka.NewSubscriber(
			sc,
			logger,
		)
	}

	return c, err
}