package sql

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	_ "github.com/lib/pq"
	"net/url"
)

func SetConf(connection string) (*sql.DB, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "mysql":
	case "oracle":
	case "postgres":
	default:
		return nil, errors.New(`scheme is undefined`)
	}

	return sql.Open(u.Scheme, u.String())
}
