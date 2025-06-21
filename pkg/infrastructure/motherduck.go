package infrastructure

import (
	"net/url"
	"strings"
)

// IsMotherDuckDSN reports whether the given DSN targets MotherDuck.
func IsMotherDuckDSN(dsn string) bool {
	u, err := url.Parse(dsn)
	if err != nil {
		return false
	}
	if u.Scheme == "motherduck" {
		return true
	}
	return u.Scheme == "duckdb" && strings.HasPrefix(u.Host, "motherduck")
}

// NormalizeMotherDuckDSN converts motherduck:// URIs to the
// duckdb://motherduck/ form understood by DuckDB.
func NormalizeMotherDuckDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	if u.Scheme == "motherduck" {
		u.Scheme = "duckdb"
		if u.Host == "" {
			u.Host = "motherduck"
		} else if !strings.HasPrefix(u.Host, "motherduck") {
			u.Host = "motherduck" + u.Host
		}
		return u.String()
	}
	return dsn
}

// InjectMotherDuckToken ensures the motherduck_token query parameter is set
// when connecting to MotherDuck. If the DSN already contains the parameter or
// the token is empty, the DSN is returned unchanged.
func InjectMotherDuckToken(dsn, token string) string {
	if token == "" {
		return dsn
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	if !IsMotherDuckDSN(dsn) {
		return dsn
	}
	q := u.Query()
	if q.Get("motherduck_token") == "" {
		q.Set("motherduck_token", token)
		u.RawQuery = q.Encode()
	}
	return u.String()
}
