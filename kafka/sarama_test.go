package kafka_test

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/artyomturkin/go-from-uri/kafka"
	"github.com/stretchr/testify/assert"
)

func TestNewSaramaConfigSuccessSimple(t *testing.T) {
	url := "kafka://broker01:9092,broker02:9092"

	brokers, conf, err := kafka.NewSaramaConfig(url)

	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"broker01:9092", "broker02:9092"}, brokers)
	assert.NotNil(t, conf)
}

func TestNewSaramaConfigWrongScheme(t *testing.T) {
	url := "kafkad://broker01:9092,broker02:9092"

	_, _, err := kafka.NewSaramaConfig(url)

	assert.Error(t, err)
}

func TestNewSaramaConfigBrokenURL(t *testing.T) {
	url := "kafka//broker01:9092,broker02:9092\\"

	_, _, err := kafka.NewSaramaConfig(url)

	assert.Error(t, err)
}

func TestNewSaramaConfigSuccessTLSAndSASL(t *testing.T) {
	url := "kafkas://user:pass@broker01:9092,broker02:9092"

	brokers, conf, err := kafka.NewSaramaConfig(url)

	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"broker01:9092", "broker02:9092"}, brokers)
	assert.NotNil(t, conf)

	// Verify TLS connection
	assert.True(t, conf.Net.TLS.Enable)

	// Verify SASL
	assert.True(t, conf.Net.SASL.Enable)
	assert.Equal(t, "user", conf.Net.SASL.User)
	assert.Equal(t, "pass", conf.Net.SASL.Password)
}

func TestNewSaramaClientErrNoBrokersToConnect(t *testing.T) {
	url := "kafkas://user:pass@broker01:9092,broker02:9092"

	_, err := kafka.NewSaramaClient(url)

	assert.ErrorIs(t, err, sarama.ErrOutOfBrokers)
}

func TestNewSaramaClientFail(t *testing.T) {
	url := "kafkas://user:pass@broker01:9092,broker02:9092\\"

	_, err := kafka.NewSaramaClient(url)

	assert.Error(t, err)
}

func TestNewSaramaConfigWithNewestOffset(t *testing.T) {
	url := "kafkas://user:pass@broker01:9092,broker02:9092?offset=newest"

	_, conf, err := kafka.NewSaramaConfig(url)

	assert.NoError(t, err)
	assert.Equal(t, sarama.OffsetNewest, conf.Consumer.Offsets.Initial)
}

func TestNewSaramaConfigWithOldestOffset(t *testing.T) {
	url := "kafkas://user:pass@broker01:9092,broker02:9092?offset=oldest"

	_, conf, err := kafka.NewSaramaConfig(url)

	assert.NoError(t, err)
	assert.Equal(t, sarama.OffsetOldest, conf.Consumer.Offsets.Initial)
}

func TestNewSaramaConfigWithCustomOffset(t *testing.T) {
	url := "kafkas://user:pass@broker01:9092,broker02:9092?offset=123"

	_, conf, err := kafka.NewSaramaConfig(url)

	assert.NoError(t, err)
	assert.Equal(t, int64(123), conf.Consumer.Offsets.Initial)
}

func TestNewSaramaConfigWithCustomOffsetFail(t *testing.T) {
	url := "kafkas://user:pass@broker01:9092,broker02:9092?offset=123a"

	_, _, err := kafka.NewSaramaConfig(url)

	assert.Error(t, err)
}
