package sql

import (
	"errors"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	stdSQL "github.com/jmoiron/sqlx"
	kafkadeduplication "github.com/qairjar/kafka-deduplication"
	"io/ioutil"
	"strconv"
	"time"

	plugin "github.com/qairjar/watermill-sql-plugin"
	"net/url"
)

var driverMap map[string]string

func init() {
	driverMap = make(map[string]string)
	driverMap["mysql"] = "sql"
	driverMap["oracle"] = "godror"
	driverMap["postgres"] = "postgres"
}

func Open(connection string) (*stdSQL.DB, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}
	driver, ok := driverMap[u.Scheme]

	if !ok {
		return nil, errors.New("schema does not exist")
	}
	dbURL := u.String()
	db, err := stdSQL.Open(driver, dbURL)
	return db, err
}

func getSelect(connection string) (string, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return "", nil
	}
	var selectQuery string
	if len(u.Query().Get("select-path")) > 0 {
		file, err := ioutil.ReadFile(u.Query().Get("select-path"))
		if err != nil {
			return "", nil
		}
		selectQuery = string(file)
	}
	return selectQuery, nil
}

func initWindow(connection string) (plugin.Window, error) {
	var window plugin.Window
	uri, err := url.Parse(connection)
	if err != nil {
		return window, err
	}
	queryURI := uri.Query()
	if len(queryURI.Get("init-from")) > 0 {
		window.InitFrom, err = time.Parse(time.RFC3339, queryURI.Get("init-from"))
		if err != nil {
			return window, err
		}
	}
	queryURI.Del("init-from")

	if len(queryURI.Get("lag-duration")) > 0 {
		window.Lag, err = time.ParseDuration(queryURI.Get("lag-duration"))
		if err != nil {
			return window, err
		}
	}
	queryURI.Del("lag-duration")

	return window, err
}

func initKafka(connection string) (*kafkadeduplication.SaramaConfig, error) {

	uri, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}
	s := &kafkadeduplication.SaramaConfig{}
	queryURI := uri.Query()
	if len(queryURI.Get("kafka-brokers")) > 0 {
		s.Brokers = queryURI.Get("kafka-brokers")
	}
	queryURI.Del("kafka-brokers")

	if len(queryURI.Get("topic-name")) > 0 {
		s.TopicName = queryURI.Get("topic-name")
	}
	queryURI.Del("topic-name")

	if len(queryURI.Get("kafka-size")) > 0 {
		s.Size, err = strconv.Atoi(queryURI.Get("kafka-size"))
		if err != nil {
			return s, err
		}
	}
	queryURI.Del("kafka-size")

	return s, nil
}

func NewSubscriber(connection string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	db, err := Open(connection)
	if err != nil {
		return nil, err
	}
	selectQuery, err := getSelect(connection)
	window, err := initWindow(connection)
	if err != nil {
		return nil, err
	}
	saramaConfig, err := initKafka(connection)
	if err != nil {
		return nil, err
	}
	sub := plugin.Subscriber{DB: db, SelectQuery: selectQuery, Window: window}
	return sub.NewSubscriber(nil, logger, saramaConfig)
}

func NewPublisher(connection string, logger watermill.LoggerAdapter) (message.Publisher, error) {
	p := &plugin.Publisher{}
	return p.NewPublisher(nil, logger)
}
