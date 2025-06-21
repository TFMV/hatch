package infrastructure

import "testing"

func TestMotherDuckDSNHelpers(t *testing.T) {
	cases := []struct {
		dsn   string
		token string
		want  string
	}{
		{"motherduck://mydb", "tok", "duckdb://motherduck/mydb?motherduck_token=tok"},
		{"duckdb://motherduck/mydb", "tok", "duckdb://motherduck/mydb?motherduck_token=tok"},
		{"duckdb://other/db", "tok", "duckdb://other/db"},
	}
	for _, c := range cases {
		norm := NormalizeMotherDuckDSN(c.dsn)
		got := InjectMotherDuckToken(norm, c.token)
		if got != c.want {
			t.Errorf("InjectMotherDuckToken(%q, %q) = %q, want %q", c.dsn, c.token, got, c.want)
		}
	}
}
