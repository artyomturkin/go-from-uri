package sql

import (
	dbSql "database/sql"
	"errors"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"io/ioutil"

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
func NewSubscriber(connection string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	db, err := Open(connection)
	if err != nil {
		return nil, err
	}
	selectQuery, err := getSelect(connection)
	sub := plugin.Subscriber{DB: db, SelectQuery:selectQuery}
	_, err = sub.InitCache(connection)
	if err != nil {
		return nil, err
	}
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
