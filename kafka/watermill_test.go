package kafka_test

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/artyomturkin/go-from-uri/kafka"
	"github.com/stretchr/testify/assert"
)

func TestWatermillPublisherNoBrokers(t *testing.T) {
	url := "kafka://broker01:9092,broker02:9092"

	_, err := kafka.NewWatermillPublisher(url, nil)

	assert.Contains(t, err.Error(), sarama.ErrOutOfBrokers.Error())
}

func TestWatermillPublisherBrokenURL(t *testing.T) {
	url := "kafka://broker01:9092,broker02:9092\\"

	_, err := kafka.NewWatermillPublisher(url, nil)

	assert.Error(t, err)
}

func TestWatermillSubscriberNoBrokers(t *testing.T) {
	url := "kafka://broker01:9092,broker02:9092?group=hello&offset=newest"

	sub, err := kafka.NewWatermillSubscriber(url, nil)

	assert.NoError(t, err)
	assert.NotNil(t, sub)
}

func TestWatermillSubscriberBrokenURL(t *testing.T) {
	url := "kafka://broker01:9092,broker02:9092\\"

	_, err := kafka.NewWatermillSubscriber(url, nil)

	assert.Error(t, err)
}
