package kafka

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/artyomturkin/go-from-uri"
)

// NewSaramaConfig build new sarama config from URL string.
func NewSaramaConfig(u *url.URL) ([]string, *sarama.Config, error) {
	conf := sarama.NewConfig()

	switch u.Scheme {
	case "kafka":
	case "kafkas":
		conf.Net.TLS.Enable = true
	default:
		return nil, nil, fromuri.ErrUnsupportedScheme
	}

	if pass, set := u.User.Password(); set {
		conf.Net.SASL.Enable = true
		conf.Net.SASL.User = u.User.Username()
		conf.Net.SASL.Password = pass
	}
	conf.Producer.Return.Errors = true
	conf.Producer.Return.Successes = true
	conf.Consumer.Return.Errors = true
	conf.Producer.RequiredAcks = sarama.WaitForAll

	switch s := u.Query().Get("offset"); s {
	case "oldest":
		conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		conf.Consumer.Offsets.Initial = sarama.OffsetNewest
	case "":
	default:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, nil, err
		}

		conf.Consumer.Offsets.Initial = i
	}

	return strings.Split(u.Host, ","), conf, nil
}

// NewSaramaConfig build new sarama client from URL string.
func NewSaramaClient(connectionURL string) (sarama.Client, error) {
	u, err := url.Parse(connectionURL)
	if err != nil {
		return nil, err
	}
	brokers, conf, err := NewSaramaConfig(u)
	if err != nil {
		return nil, err
	}

	return sarama.NewClient(brokers, conf)
}
