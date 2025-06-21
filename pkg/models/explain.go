package models

// ExplainResult represents a query plan summary returned by a backend.
type ExplainResult struct {
	Backend       string `json:"backend"`
	PlanSummary   string `json:"plan_summary"`
	EstimatedRows int    `json:"estimated_rows"`
	ExecutionMode string `json:"execution_mode"`
	CacheStatus   string `json:"cache_status"`
}
