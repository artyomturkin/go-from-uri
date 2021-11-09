package postgres

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	_ "github.com/lib/pq"
	"net/url"
)

func SetConf(u *url.URL) (*sql.DB, error) {
	return sql.Open(u.Scheme, u.String())
}
