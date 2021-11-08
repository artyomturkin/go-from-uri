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

	scyllaConf.Host = u.Host
	scyllaConf.Keyspace = u.Query().Get("keyspace")
	scyllaConf.User = u.User.Username()
	scyllaConf.Type = u.Query().Get("type")
	scyllaConf.Consistency = u.Query().Get("consistency")
	scyllaConf.EnableAuth, _ = strconv.ParseBool(u.Query().Get("enableAuth"))
	scyllaConf.Pass, _ = u.User.Password()
	scyllaConf.TimeoutValid, _ = time.ParseDuration(u.Query().Get("timeoutValid"))
	scyllaConf.ConnectTimeout, _ = time.ParseDuration(u.Query().Get("connectTimeout"))

	return scyllaConf.CreateDB()
}
