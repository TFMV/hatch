package services

import (
	"testing"
)

func TestStatementClassifier_ClassifyStatement(t *testing.T) {
	classifier := NewStatementClassifier()

	tests := []struct {
		name     string
		sql      string
		expected StatementType
	}{
		// DDL statements
		{"CREATE TABLE", "CREATE TABLE test (id INT)", StatementTypeDDL},
		{"CREATE INDEX", "CREATE INDEX idx_test ON test(id)", StatementTypeDDL},
		{"DROP TABLE", "DROP TABLE test", StatementTypeDDL},
		{"DROP INDEX", "DROP INDEX idx_test", StatementTypeDDL},
		{"ALTER TABLE", "ALTER TABLE test ADD COLUMN name VARCHAR(50)", StatementTypeDDL},
		{"TRUNCATE", "TRUNCATE TABLE test", StatementTypeDDL},
		{"CREATE with whitespace", "  CREATE TABLE test2 (id INT)  ", StatementTypeDDL},
		{"CREATE lowercase", "create table test3 (id int)", StatementTypeDDL},

		// DML statements
		{"INSERT", "INSERT INTO test VALUES (1)", StatementTypeDML},
		{"UPDATE", "UPDATE test SET id = 2", StatementTypeDML},
		{"DELETE", "DELETE FROM test WHERE id = 1", StatementTypeDML},
		{"REPLACE", "REPLACE INTO test VALUES (1)", StatementTypeDML},
		{"MERGE", "MERGE INTO test USING source ON test.id = source.id", StatementTypeDML},
		{"INSERT with whitespace", "  INSERT INTO test VALUES (2)  ", StatementTypeDML},
		{"UPDATE lowercase", "update test set id = 3", StatementTypeDML},

		// DQL statements
		{"SELECT", "SELECT * FROM test", StatementTypeDQL},
		{"SELECT with JOIN", "SELECT t1.*, t2.* FROM test t1 JOIN test2 t2 ON t1.id = t2.id", StatementTypeDQL},
		{"WITH CTE", "WITH cte AS (SELECT * FROM test) SELECT * FROM cte", StatementTypeDQL},
		{"SELECT with whitespace", "  SELECT * FROM test  ", StatementTypeDQL},
		{"SELECT lowercase", "select * from test", StatementTypeDQL},

		// Utility statements
		{"SHOW", "SHOW TABLES", StatementTypeUtility},
		{"DESCRIBE", "DESCRIBE test", StatementTypeUtility},
		{"DESC", "DESC test", StatementTypeUtility},
		{"EXPLAIN", "EXPLAIN SELECT * FROM test", StatementTypeUtility},
		{"ANALYZE", "ANALYZE TABLE test", StatementTypeUtility},
		{"SET", "SET autocommit = 1", StatementTypeUtility},
		{"USE", "USE database_name", StatementTypeUtility},
		{"PRAGMA", "PRAGMA table_info(test)", StatementTypeUtility},

		// Edge cases
		{"Empty string", "", StatementTypeOther},
		{"Whitespace only", "   ", StatementTypeOther},
		{"Unknown statement", "UNKNOWN STATEMENT", StatementTypeOther},
		{"Comment only", "-- This is a comment", StatementTypeOther},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifier.ClassifyStatement(tt.sql)
			if result != tt.expected {
				t.Errorf("ClassifyStatement(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestStatementClassifier_IsUpdateStatement(t *testing.T) {
	classifier := NewStatementClassifier()

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		// Should return true for DDL and DML
		{"CREATE TABLE", "CREATE TABLE test (id INT)", true},
		{"DROP TABLE", "DROP TABLE test", true},
		{"ALTER TABLE", "ALTER TABLE test ADD COLUMN name VARCHAR(50)", true},
		{"INSERT", "INSERT INTO test VALUES (1)", true},
		{"UPDATE", "UPDATE test SET id = 2", true},
		{"DELETE", "DELETE FROM test WHERE id = 1", true},
		{"TRUNCATE", "TRUNCATE TABLE test", true},

		// Should return false for DQL and Utility
		{"SELECT", "SELECT * FROM test", false},
		{"WITH CTE", "WITH cte AS (SELECT * FROM test) SELECT * FROM cte", false},
		{"SHOW", "SHOW TABLES", false},
		{"DESCRIBE", "DESCRIBE test", false},
		{"EXPLAIN", "EXPLAIN SELECT * FROM test", false},
		{"Empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifier.IsUpdateStatement(tt.sql)
			if result != tt.expected {
				t.Errorf("IsUpdateStatement(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestStatementClassifier_IsQueryStatement(t *testing.T) {
	classifier := NewStatementClassifier()

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		// Should return true for DQL only
		{"SELECT", "SELECT * FROM test", true},
		{"SELECT with JOIN", "SELECT t1.*, t2.* FROM test t1 JOIN test2 t2 ON t1.id = t2.id", true},
		{"WITH CTE", "WITH cte AS (SELECT * FROM test) SELECT * FROM cte", true},

		// Should return false for DDL, DML, and Utility
		{"CREATE TABLE", "CREATE TABLE test (id INT)", false},
		{"INSERT", "INSERT INTO test VALUES (1)", false},
		{"UPDATE", "UPDATE test SET id = 2", false},
		{"DELETE", "DELETE FROM test WHERE id = 1", false},
		{"SHOW", "SHOW TABLES", false},
		{"DESCRIBE", "DESCRIBE test", false},
		{"Empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifier.IsQueryStatement(tt.sql)
			if result != tt.expected {
				t.Errorf("IsQueryStatement(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestStatementClassifier_GetExpectedResponseType(t *testing.T) {
	classifier := NewStatementClassifier()

	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		// DDL and DML should return UPDATE_COUNT
		{"CREATE TABLE", "CREATE TABLE test (id INT)", "UPDATE_COUNT"},
		{"INSERT", "INSERT INTO test VALUES (1)", "UPDATE_COUNT"},
		{"UPDATE", "UPDATE test SET id = 2", "UPDATE_COUNT"},
		{"DELETE", "DELETE FROM test WHERE id = 1", "UPDATE_COUNT"},

		// DQL should return RESULT_SET
		{"SELECT", "SELECT * FROM test", "RESULT_SET"},
		{"WITH CTE", "WITH cte AS (SELECT * FROM test) SELECT * FROM cte", "RESULT_SET"},

		// Utility should return RESULT_SET (default)
		{"SHOW", "SHOW TABLES", "RESULT_SET"},
		{"DESCRIBE", "DESCRIBE test", "RESULT_SET"},
		{"Empty string", "", "RESULT_SET"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifier.GetExpectedResponseType(tt.sql)
			if result != tt.expected {
				t.Errorf("GetExpectedResponseType(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestStatementType_String(t *testing.T) {
	tests := []struct {
		stmtType StatementType
		expected string
	}{
		{StatementTypeDDL, "DDL"},
		{StatementTypeDML, "DML"},
		{StatementTypeDQL, "DQL"},
		{StatementTypeOther, "OTHER"},
		{StatementType(999), "UNKNOWN"}, // Invalid type
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.stmtType.String()
			if result != tt.expected {
				t.Errorf("StatementType(%d).String() = %v, want %v", tt.stmtType, result, tt.expected)
			}
		})
	}
}

// Benchmark tests
func BenchmarkStatementClassifier_ClassifyStatement(b *testing.B) {
	classifier := NewStatementClassifier()
	testQueries := []string{
		"CREATE TABLE test (id INT)",
		"INSERT INTO test VALUES (1)",
		"SELECT * FROM test",
		"UPDATE test SET id = 2",
		"DELETE FROM test WHERE id = 1",
		"SHOW TABLES",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := testQueries[i%len(testQueries)]
		classifier.ClassifyStatement(query)
	}
}
