package duckdb

import "testing"

func TestParseDuckDBExplain(t *testing.T) {
	lines := []string{
		"Seq Scan on events (cost=0..1 rows=10)",
	}
	res := parseDuckDBExplain(lines)
	if res.EstimatedRows != 10 {
		t.Errorf("expected estimated rows 10, got %d", res.EstimatedRows)
	}
	if res.Backend != "duckdb" {
		t.Errorf("expected backend duckdb")
	}
	if res.PlanSummary == "" {
		t.Errorf("expected plan summary")
	}
}
