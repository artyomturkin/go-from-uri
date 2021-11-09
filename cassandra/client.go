package cassandra

import (
	"database/sql"
	"github.com/qairjar/watermill-scylla-plugin"
	"net/url"
	"strconv"
	"time"
)

func NewScyllaConfig(u *url.URL) (*sql.DB, error) {
	scyllaConf := scyllaplugin.SQLConfig{}

	keyspace := "test"
	dbType := "cql"
	consistency := "one"
	enableAuth := false
	timeoutValid := time.Minute * 10
	connectTimeout := time.Minute * 10

	scyllaConf.Host = u.Host
	query := u.Query()
	if query.Get("keyspace") != "" {
		keyspace = query.Get("keyspace")
	}

	if query.Get("type") != "" {
		dbType = query.Get("type")
	}

	if query.Get("consistency") != "" {
		dbType = query.Get("consistency")
	}
	var err error
	if query.Get("enableAuth") == "true" {
		enableAuth, err = strconv.ParseBool(query.Get("enableAuth"))
		if err != nil {
			return nil, err
		}

		scyllaConf.User = u.User.Username()
		scyllaConf.Pass, _ = u.User.Password()
	}

	if u.Query().Get("timeoutValid") != "" {
		timeoutValid, err = time.ParseDuration(u.Query().Get("timeoutValid"))
		if err != nil {
			return nil, err
		}
	}
	if u.Query().Get("connectTimeout") != "" {
		connectTimeout, err = time.ParseDuration(u.Query().Get("timeoutValid"))
		if err != nil {
			return nil, err
		}
	}

	scyllaConf.Type = dbType
	scyllaConf.Keyspace = keyspace
	scyllaConf.Consistency = consistency
	scyllaConf.EnableAuth = enableAuth
	scyllaConf.TimeoutValid = timeoutValid
	scyllaConf.ConnectTimeout = connectTimeout

	return scyllaConf.CreateDB()
}
