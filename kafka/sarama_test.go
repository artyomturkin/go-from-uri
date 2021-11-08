package kafka_test

import (
	"net/url"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/artyomturkin/go-from-uri/kafka"
	"github.com/stretchr/testify/assert"
)

func TestNewSaramaConfigSuccessSimple(t *testing.T) {
	connection := "kafka://broker01:9092,broker02:9092"

	u, _ := url.Parse(connection)
	brokers, conf, err := kafka.NewSaramaConfig(u)

	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"broker01:9092", "broker02:9092"}, brokers)
	assert.NotNil(t, conf)
}

func TestNewSaramaConfigWrongScheme(t *testing.T) {
	connection := "kafkad://broker01:9092,broker02:9092"

	u, _ := url.Parse(connection)
	_, _, err := kafka.NewSaramaConfig(u)
	assert.Error(t, err)
}

func TestNewSaramaConfigBrokenURL(t *testing.T) {
	connection := "kafka//broker01:9092,broker02:9092\\"
	u, _:= url.Parse(connection)
	_, _, err := kafka.NewSaramaConfig(u)

	assert.Error(t, err)
}

func TestNewSaramaConfigSuccessTLSAndSASL(t *testing.T) {
	connection := "kafkas://user:pass@broker01:9092,broker02:9092"

	u, _ := url.Parse(connection)
	brokers, conf, err := kafka.NewSaramaConfig(u)

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
	connection := "kafkas://user:pass@broker01:9092,broker02:9092"

	_, err := kafka.NewSaramaClient(connection)

	assert.ErrorIs(t, err, sarama.ErrOutOfBrokers)
}

func TestNewSaramaClientFail(t *testing.T) {
	connection := "kafkas://user:pass@broker01:9092,broker02:9092\\"
	_, err := kafka.NewSaramaClient(connection)

	assert.Error(t, err)
}

func TestNewSaramaConfigWithNewestOffset(t *testing.T) {
	connection := "kafkas://user:pass@broker01:9092,broker02:9092?offset=newest"
	u, _ := url.Parse(connection)
	_, conf, err := kafka.NewSaramaConfig(u)

	assert.NoError(t, err)
	assert.Equal(t, sarama.OffsetNewest, conf.Consumer.Offsets.Initial)
}

func TestNewSaramaConfigWithOldestOffset(t *testing.T) {
	connection := "kafkas://user:pass@broker01:9092,broker02:9092?offset=oldest"
	u, _ := url.Parse(connection)
	_, conf, err := kafka.NewSaramaConfig(u)

	assert.NoError(t, err)
	assert.Equal(t, sarama.OffsetOldest, conf.Consumer.Offsets.Initial)
}

func TestNewSaramaConfigWithCustomOffset(t *testing.T) {
	connection := "kafkas://user:pass@broker01:9092,broker02:9092?offset=123"

	u, _ := url.Parse(connection)
	_, conf, err := kafka.NewSaramaConfig(u)

	assert.NoError(t, err)
	assert.Equal(t, int64(123), conf.Consumer.Offsets.Initial)
}

func TestNewSaramaConfigWithCustomOffsetFail(t *testing.T) {
	connection := "kafkas://user:pass@broker01:9092,broker02:9092?offset=123a"

	u, _ := url.Parse(connection)
	_, _, err := kafka.NewSaramaConfig(u)

	assert.Error(t, err)
}
