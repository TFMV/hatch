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
		// Additional test cases for edge scenarios
		{"motherduck://", "tok", "duckdb://motherduck?motherduck_token=tok"},
		{"motherduck://mydb/path", "tok", "duckdb://motherduck/mydb/path?motherduck_token=tok"},
		{"motherduck://mydb", "", "duckdb://motherduck/mydb"}, // empty token
	}
	for _, c := range cases {
		norm := NormalizeMotherDuckDSN(c.dsn)
		got := InjectMotherDuckToken(norm, c.token)
		if got != c.want {
			t.Errorf("InjectMotherDuckToken(%q, %q) = %q, want %q", c.dsn, c.token, got, c.want)
		}
	}
}

func TestNormalizeMotherDuckDSN(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"motherduck://mydb", "duckdb://motherduck/mydb"},
		{"motherduck://", "duckdb://motherduck"},
		{"motherduck://mydb/path", "duckdb://motherduck/mydb/path"},
		{"duckdb://motherduck/mydb", "duckdb://motherduck/mydb"}, // already normalized
		{"duckdb://other/db", "duckdb://other/db"},               // not motherduck
		{"invalid://url", "invalid://url"},                       // invalid URL should pass through
	}
	for _, c := range cases {
		got := NormalizeMotherDuckDSN(c.input)
		if got != c.want {
			t.Errorf("NormalizeMotherDuckDSN(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}
