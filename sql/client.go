package sql

import (
	dbSql "database/sql"
	"errors"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
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

func Open(connection string) (*dbSql.DB, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}
	driver, ok := driverMap[u.Scheme]

	if !ok {
		return nil, errors.New("schema does not exist")
	}
	dbURL := u.String()
	db, err := dbSql.Open(driver, dbURL)
	return db, err
}

func NewSubscriber(connection string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	db, err := Open(connection)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(connection)
	sub := plugin.Subscriber{DB: db, SelectPath: u.Query().Get("sql-path")}
	return sub.NewSubscriber(nil, logger)
}

func NewPublisher(connection string, logger watermill.LoggerAdapter) (message.Publisher, error) {
	db, err := Open(connection)
	if err != nil {
		return nil, err
	}
	p := &plugin.Publisher{DB: db}
	return p.NewPublisher(nil, logger)
}
