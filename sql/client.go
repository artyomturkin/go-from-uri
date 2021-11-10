package sql

import (
	dbSql "database/sql"
	"errors"
	"net/url"
)

var driverMap map[string]string

func init(){
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
	driver,ok := driverMap[u.Scheme]

	if !ok {
		return nil, errors.New("schema does not exist")
	}
	dbURL := u.String()
	return dbSql.Open(driver, dbURL)
}
