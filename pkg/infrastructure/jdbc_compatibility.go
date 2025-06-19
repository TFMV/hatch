// Package infrastructure provides enterprise-grade JDBC compatibility for Porter.
package infrastructure

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/TFMV/porter/pkg/errors"
	"github.com/TFMV/porter/pkg/services"
)

// JDBCCompatibilityLayer provides enterprise-grade JDBC compatibility features.
type JDBCCompatibilityLayer struct {
	allocator           memory.Allocator
	logger              zerolog.Logger
	statementClassifier *services.EnterpriseStatementClassifier
	sqlInfoProvider     *EnhancedSQLInfoProvider
	errorMapper         *JDBCErrorMapper
	metrics             JDBCMetrics
	config              JDBCConfig
}

// JDBCConfig holds configuration for JDBC compatibility features.
type JDBCConfig struct {
	StrictCompliance        bool          `yaml:"strict_compliance"`
	EnableStatementCaching  bool          `yaml:"enable_statement_caching"`
	StatementCacheSize      int           `yaml:"statement_cache_size"`
	StatementTimeout        time.Duration `yaml:"statement_timeout"`
	MaxStatementLength      int           `yaml:"max_statement_length"`
	EnableQueryValidation   bool          `yaml:"enable_query_validation"`
	EnableMetricsCollection bool          `yaml:"enable_metrics_collection"`
	ErrorDetailLevel        string        `yaml:"error_detail_level"` // "minimal", "standard", "verbose"
}

// JDBCMetrics interface for JDBC-specific metrics collection.
// This interface is implemented by EnterpriseJDBCMetrics for full functionality.
type JDBCMetrics interface {
	IncrementStatementCount(stmtType string)
	RecordStatementExecutionTime(stmtType string, duration time.Duration)
	IncrementErrorCount(errorType string)
	RecordRowsAffected(count int64)
	IncrementCacheHit()
	IncrementCacheMiss()

	// Extended methods for enterprise features
	RecordQueryComplexity(stmtType string, complexityScore float64)
	RecordResultSetSize(stmtType string, sizeBytes int64)
	UpdateActiveConnections(count int)
	UpdateActiveTransactions(count int)
	UpdateCacheSize(size int)
	UpdateMemoryUsage(bytes int64)
}

// NewJDBCCompatibilityLayer creates a new enterprise-grade JDBC compatibility layer.
func NewJDBCCompatibilityLayer(
	allocator memory.Allocator,
	logger zerolog.Logger,
	config JDBCConfig,
	metrics JDBCMetrics,
) *JDBCCompatibilityLayer {
	return &JDBCCompatibilityLayer{
		allocator:           allocator,
		logger:              logger.With().Str("component", "jdbc-compatibility").Logger(),
		statementClassifier: services.NewEnterpriseStatementClassifier(),
		sqlInfoProvider:     NewEnhancedSQLInfoProvider(allocator),
		errorMapper:         NewJDBCErrorMapper(config.ErrorDetailLevel),
		metrics:             metrics,
		config:              config,
	}
}

// ValidateStatement performs enterprise-grade SQL statement validation.
func (j *JDBCCompatibilityLayer) ValidateStatement(ctx context.Context, sql string) error {
	if !j.config.EnableQueryValidation {
		return nil
	}

	// Use the enhanced statement classifier for validation
	if err := j.statementClassifier.ValidateStatement(sql); err != nil {
		return j.errorMapper.MapError(fmt.Errorf("validation failed: %w", err))
	}

	// Length validation
	if j.config.MaxStatementLength > 0 && len(sql) > j.config.MaxStatementLength {
		return j.errorMapper.MapError(errors.New(errors.CodeInvalidRequest,
			fmt.Sprintf("SQL statement exceeds maximum length of %d characters", j.config.MaxStatementLength)))
	}

	// Enhanced security validation using statement classifier
	info, err := j.statementClassifier.AnalyzeStatement(sql)
	if err != nil {
		return j.errorMapper.MapError(fmt.Errorf("analysis failed: %w", err))
	}

	// Check for dangerous operations in strict compliance mode
	if j.config.StrictCompliance {
		if info.IsDangerous {
			return j.errorMapper.MapError(errors.New(errors.CodePermissionDenied,
				"dangerous operation not allowed in strict compliance mode"))
		}

		if info.HasSQLInjectionRisk {
			return j.errorMapper.MapError(errors.New(errors.CodePermissionDenied,
				"potential SQL injection detected"))
		}

		if info.SecurityRisk == services.SecurityRiskHigh || info.SecurityRisk == services.SecurityRiskCritical {
			return j.errorMapper.MapError(errors.New(errors.CodePermissionDenied,
				fmt.Sprintf("security risk level %s not allowed", info.SecurityRisk.String())))
		}
	}

	return nil
}

// ClassifyStatementForJDBC classifies SQL statements with JDBC-specific handling.
func (j *JDBCCompatibilityLayer) ClassifyStatementForJDBC(sql string) (services.StatementType, JDBCStatementInfo, error) {
	// Use the enhanced statement classifier
	info, err := j.statementClassifier.AnalyzeStatement(sql)
	if err != nil {
		return services.StatementTypeOther, JDBCStatementInfo{}, err
	}

	jdbcInfo := JDBCStatementInfo{
		Type:                info.Type,
		ExpectsResultSet:    info.ExpectsResultSet,
		ExpectsUpdateCount:  info.ExpectsUpdateCount,
		RequiresTransaction: info.RequiresTransaction,
		IsReadOnly:          info.IsReadOnly,
		EstimatedComplexity: info.Complexity,
		SecurityRisk:        info.SecurityRisk,
		IsDangerous:         info.IsDangerous,
		Keywords:            info.Keywords,
		Tables:              info.Tables,
		Operations:          info.Operations,
	}

	// Record metrics
	if j.config.EnableMetricsCollection && j.metrics != nil {
		j.metrics.IncrementStatementCount(info.Type.String())

		// Record complexity if supported
		if complexityRecorder, ok := j.metrics.(interface {
			RecordQueryComplexity(string, float64)
		}); ok {
			complexityRecorder.RecordQueryComplexity(info.Type.String(), float64(info.Complexity))
		}
	}

	return info.Type, jdbcInfo, nil
}

// CreateJDBCCompatibleFlightInfo creates FlightInfo with JDBC-compatible metadata.
func (j *JDBCCompatibilityLayer) CreateJDBCCompatibleFlightInfo(
	ctx context.Context,
	sql string,
	schema *arrow.Schema,
	stmtType services.StatementType,
) (*flight.FlightInfo, error) {

	// Create descriptor
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(sql),
	}

	var resultSchema *arrow.Schema
	var endpoints []*flight.FlightEndpoint

	if stmtType == services.StatementTypeDDL || stmtType == services.StatementTypeDML ||
		stmtType == services.StatementTypeTCL || stmtType == services.StatementTypeDCL {
		// For DDL/DML/TCL/DCL, create update count schema
		resultSchema = j.createUpdateCountSchema()
		endpoints = []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte(sql)},
		}}
	} else {
		// For DQL/Utility, use provided schema
		resultSchema = schema
		endpoints = []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte(sql)},
		}}
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(resultSchema, j.allocator),
		FlightDescriptor: desc,
		Endpoint:         endpoints,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

// CreateUpdateCountResult creates a JDBC-compatible update count result.
func (j *JDBCCompatibilityLayer) CreateUpdateCountResult(rowsAffected int64) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := j.createUpdateCountSchema()

	// Create record with update count
	builder := array.NewRecordBuilder(j.allocator, schema)
	defer builder.Release()

	int64Builder := builder.Field(0).(*array.Int64Builder)
	int64Builder.Append(rowsAffected)

	record := builder.NewRecord()
	// Note: Don't release record here as it will be used by the consumer

	// Create channel with result
	resultChan := make(chan flight.StreamChunk, 1)
	resultChan <- flight.StreamChunk{Data: record}
	close(resultChan)

	// Record metrics
	if j.config.EnableMetricsCollection && j.metrics != nil {
		j.metrics.RecordRowsAffected(rowsAffected)
	}

	return schema, resultChan, nil
}

// GetEnhancedSQLInfo returns comprehensive SQL information for JDBC clients.
func (j *JDBCCompatibilityLayer) GetEnhancedSQLInfo(infoTypes []uint32) (arrow.Record, error) {
	return j.sqlInfoProvider.GetSQLInfo(infoTypes)
}

// JDBCStatementInfo provides detailed information about SQL statements for JDBC compatibility.
type JDBCStatementInfo struct {
	Type                services.StatementType
	ExpectsResultSet    bool
	ExpectsUpdateCount  bool
	RequiresTransaction bool
	IsReadOnly          bool
	EstimatedComplexity services.StatementComplexity
	SecurityRisk        services.SecurityRiskLevel
	IsDangerous         bool
	Keywords            []string
	Tables              []string
	Operations          []string
}

// createUpdateCountSchema creates the schema for update count results.
func (j *JDBCCompatibilityLayer) createUpdateCountSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "rows_affected", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

// JDBCErrorMapper provides enterprise-grade error mapping for JDBC compatibility.
type JDBCErrorMapper struct {
	detailLevel string
}

// NewJDBCErrorMapper creates a new JDBC error mapper.
func NewJDBCErrorMapper(detailLevel string) *JDBCErrorMapper {
	return &JDBCErrorMapper{
		detailLevel: detailLevel,
	}
}

// MapError maps internal errors to JDBC-compatible error messages.
func (m *JDBCErrorMapper) MapError(err error) error {
	if err == nil {
		return nil
	}

	// Extract error details based on configuration
	switch m.detailLevel {
	case "minimal":
		return m.createMinimalError(err)
	case "verbose":
		return m.createVerboseError(err)
	default: // "standard"
		return m.createStandardError(err)
	}
}

// createMinimalError creates a minimal error message for security.
func (m *JDBCErrorMapper) createMinimalError(err error) error {
	return fmt.Errorf("SQL execution failed")
}

// createStandardError creates a standard error message with basic details.
func (m *JDBCErrorMapper) createStandardError(err error) error {
	var flightErr *errors.FlightError
	if stderrors.As(err, &flightErr) {
		switch flightErr.Code {
		case errors.CodeInvalidRequest:
			return fmt.Errorf("invalid SQL statement: %s", flightErr.Message)
		case errors.CodeNotFound:
			return fmt.Errorf("object not found: %s", flightErr.Message)
		case errors.CodePermissionDenied:
			return fmt.Errorf("access denied: %s", flightErr.Message)
		case errors.CodeQueryFailed:
			return fmt.Errorf("query execution failed: %s", flightErr.Message)
		default:
			return fmt.Errorf("database error: %s", flightErr.Message)
		}
	}
	return fmt.Errorf("database error: %s", err.Error())
}

// createVerboseError creates a detailed error message for debugging.
func (m *JDBCErrorMapper) createVerboseError(err error) error {
	return fmt.Errorf("detailed error: %w", err)
}

// EnhancedSQLInfoProvider provides comprehensive SQL information for enterprise JDBC clients.
type EnhancedSQLInfoProvider struct {
	allocator memory.Allocator
	sqlInfo   map[uint32]interface{}
	mu        sync.RWMutex
}

// NewEnhancedSQLInfoProvider creates a new enhanced SQL info provider.
func NewEnhancedSQLInfoProvider(allocator memory.Allocator) *EnhancedSQLInfoProvider {
	provider := &EnhancedSQLInfoProvider{
		allocator: allocator,
		sqlInfo:   make(map[uint32]interface{}),
	}
	provider.initializeEnhancedSQLInfo()
	return provider
}

// initializeEnhancedSQLInfo sets up comprehensive SQL information.
func (p *EnhancedSQLInfoProvider) initializeEnhancedSQLInfo() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Server identification
	p.sqlInfo[uint32(flightsql.SqlInfoFlightSqlServerName)] = "Porter Enterprise"
	p.sqlInfo[uint32(flightsql.SqlInfoFlightSqlServerVersion)] = "2.0.0"
	p.sqlInfo[uint32(flightsql.SqlInfoFlightSqlServerArrowVersion)] = "18.0.0"
	p.sqlInfo[uint32(flightsql.SqlInfoFlightSqlServerReadOnly)] = false
	p.sqlInfo[uint32(flightsql.SqlInfoFlightSqlServerTransaction)] = int32(flightsql.SqlTransactionTransaction)

	// Enhanced capabilities
	p.sqlInfo[uint32(flightsql.SqlInfoDDLCatalog)] = true
	p.sqlInfo[uint32(flightsql.SqlInfoDDLSchema)] = true
	p.sqlInfo[uint32(flightsql.SqlInfoDDLTable)] = true
	p.sqlInfo[uint32(flightsql.SqlInfoAllTablesAreASelectable)] = true
	p.sqlInfo[uint32(flightsql.SqlInfoTransactionsSupported)] = true

	// JDBC-specific features (using available constants)
	p.sqlInfo[uint32(flightsql.SqlInfoSupportsLikeEscapeClause)] = true
	p.sqlInfo[uint32(flightsql.SqlInfoSupportsNonNullableColumns)] = true

	// Enhanced keyword and function lists from statement classifier
	p.sqlInfo[uint32(flightsql.SqlInfoKeywords)] = p.getEnterpriseKeywords()
	p.sqlInfo[uint32(flightsql.SqlInfoNumericFunctions)] = p.getEnterpriseNumericFunctions()
	p.sqlInfo[uint32(flightsql.SqlInfoStringFunctions)] = p.getEnterpriseStringFunctions()
}

// GetSQLInfo returns SQL information for the given info types.
func (p *EnhancedSQLInfoProvider) GetSQLInfo(infoTypes []uint32) (arrow.Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Create schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "info_name", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	// Build arrays
	builder := array.NewRecordBuilder(p.allocator, schema)
	defer builder.Release()

	infoNameBuilder := builder.Field(0).(*array.Uint32Builder)
	valueBuilder := builder.Field(1).(*array.StringBuilder)

	// If no specific info types requested, return all
	if len(infoTypes) == 0 {
		for info := range p.sqlInfo {
			infoTypes = append(infoTypes, info)
		}
	}

	// Add requested info
	for _, infoType := range infoTypes {
		if value, exists := p.sqlInfo[infoType]; exists {
			infoNameBuilder.Append(infoType)
			valueBuilder.Append(p.formatValue(value))
		}
	}

	return builder.NewRecord(), nil
}

// getEnterpriseKeywords returns comprehensive SQL keywords for enterprise clients.
func (p *EnhancedSQLInfoProvider) getEnterpriseKeywords() []string {
	return []string{
		"ABORT", "ABSOLUTE", "ACCESS", "ACTION", "ADD", "ADMIN", "AFTER", "AGGREGATE",
		"ALL", "ALSO", "ALTER", "ALWAYS", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY",
		"AS", "ASC", "ASSERTION", "ASSIGNMENT", "ASYMMETRIC", "AT", "ATTACH", "ATTRIBUTE",
		"AUTHORIZATION", "BACKWARD", "BEFORE", "BEGIN", "BETWEEN", "BIGINT", "BINARY",
		"BIT", "BOOLEAN", "BOTH", "BY", "CACHE", "CALL", "CALLED", "CASCADE", "CASCADED",
		"CASE", "CAST", "CATALOG", "CHAIN", "CHAR", "CHARACTER", "CHARACTERISTICS",
		"CHECK", "CHECKPOINT", "CLASS", "CLOSE", "CLUSTER", "COALESCE", "COLLATE",
		"COLLATION", "COLUMN", "COLUMNS", "COMMENT", "COMMENTS", "COMMIT", "COMMITTED",
		"CONCURRENTLY", "CONFIGURATION", "CONFLICT", "CONNECTION", "CONSTRAINT",
		"CONSTRAINTS", "CONTENT", "CONTINUE", "CONVERSION", "COPY", "COST", "CREATE",
		"CROSS", "CSV", "CUBE", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE",
		"CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"CURRENT_USER", "CURSOR", "CYCLE", "DATA", "DATABASE", "DAY", "DAYS",
		"DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFAULTS", "DEFERRABLE",
		"DEFERRED", "DEFINER", "DELETE", "DELIMITER", "DELIMITERS", "DEPENDS", "DESC",
		"DESCRIBE", "DETACH", "DICTIONARY", "DISABLE", "DISCARD", "DISTINCT", "DO",
		"DOCUMENT", "DOMAIN", "DOUBLE", "DROP", "EACH", "ELSE", "ENABLE", "ENCODING",
		"ENCRYPTED", "END", "ENUM", "ESCAPE", "EVENT", "EXCEPT", "EXCLUDE", "EXCLUDING",
		"EXCLUSIVE", "EXECUTE", "EXISTS", "EXPLAIN", "EXPORT", "EXTENSION", "EXTERNAL",
		"EXTRACT", "FALSE", "FAMILY", "FETCH", "FILTER", "FIRST", "FLOAT", "FOLLOWING",
		"FOR", "FORCE", "FOREIGN", "FORWARD", "FREEZE", "FROM", "FULL", "FUNCTION",
		"FUNCTIONS", "GENERATED", "GLOB", "GLOBAL", "GRANT", "GRANTED", "GROUP",
		"GROUPING", "HANDLER", "HAVING", "HEADER", "HOLD", "HOUR", "HOURS", "IDENTITY",
		"IF", "ILIKE", "IMMEDIATE", "IMMUTABLE", "IMPLICIT", "IMPORT", "IN", "INCLUDING",
		"INCREMENT", "INDEX", "INDEXES", "INHERIT", "INHERITS", "INITIALLY", "INLINE",
		"INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INSTEAD", "INT", "INTEGER",
		"INTERSECT", "INTERVAL", "INTO", "INVOKER", "IS", "ISNULL", "ISOLATION", "JOIN",
		"KEY", "LABEL", "LANGUAGE", "LARGE", "LAST", "LATERAL", "LEADING", "LEAKPROOF",
		"LEFT", "LEVEL", "LIKE", "LIMIT", "LISTEN", "LOAD", "LOCAL", "LOCALTIME",
		"LOCALTIMESTAMP", "LOCATION", "LOCK", "LOCKED", "LOGGED", "MACRO", "MAP",
		"MAPPING", "MATCH", "MATERIALIZED", "MAXVALUE", "METHOD", "MICROSECOND",
		"MICROSECONDS", "MILLISECOND", "MILLISECONDS", "MINUTE", "MINUTES", "MINVALUE",
		"MODE", "MONTH", "MONTHS", "MOVE", "NAME", "NAMES", "NATIONAL", "NATURAL",
		"NCHAR", "NEW", "NEXT", "NO", "NONE", "NOT", "NOTHING", "NOTIFY", "NOTNULL",
		"NOWAIT", "NULL", "NULLIF", "NULLS", "NUMERIC", "OBJECT", "OF", "OFF", "OFFSET",
		"OIDS", "OLD", "ON", "ONLY", "OPERATOR", "OPTION", "OPTIONS", "OR", "ORDER",
		"ORDINALITY", "OUT", "OUTER", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING",
		"OWNED", "OWNER", "PARALLEL", "PARSER", "PARTIAL", "PARTITION", "PASSING",
		"PASSWORD", "PERCENT", "PLACING", "PLANS", "POLICY", "POSITION", "PRAGMA",
		"PRECEDING", "PRECISION", "PREPARE", "PREPARED", "PRESERVE", "PRIMARY", "PRIOR",
		"PRIVILEGES", "PROCEDURAL", "PROCEDURE", "PROGRAM", "PUBLICATION", "QUOTE",
		"RANGE", "READ", "REAL", "REASSIGN", "RECHECK", "RECURSIVE", "REF", "REFERENCES",
		"REFERENCING", "REFRESH", "REINDEX", "RELATIVE", "RELEASE", "RENAME",
		"REPEATABLE", "REPLACE", "REPLICA", "RESET", "RESTART", "RESTRICT", "RETURNING",
		"RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROW", "ROWS",
		"RULE", "SAMPLE", "SAVEPOINT", "SCHEMA", "SCHEMAS", "SCROLL", "SEARCH", "SECOND",
		"SECONDS", "SECURITY", "SELECT", "SEQUENCE", "SEQUENCES", "SERIALIZABLE",
		"SERVER", "SESSION", "SESSION_USER", "SET", "SETOF", "SETS", "SHARE", "SHOW",
		"SIMILAR", "SIMPLE", "SKIP", "SMALLINT", "SNAPSHOT", "SOME", "SQL", "STABLE",
		"STANDALONE", "START", "STATEMENT", "STATISTICS", "STDIN", "STDOUT", "STORAGE",
		"STRICT", "STRIP", "STRUCT", "SUBSCRIPTION", "SUBSTRING", "SYMMETRIC", "SYSID",
		"SYSTEM", "TABLE", "TABLES", "TABLESAMPLE", "TABLESPACE", "TEMP", "TEMPLATE",
		"TEMPORARY", "TEXT", "THEN", "TIME", "TIMESTAMP", "TO", "TRAILING", "TRANSACTION",
		"TRANSFORM", "TREAT", "TRIGGER", "TRIM", "TRUE", "TRUNCATE", "TRUSTED", "TRY_CAST",
		"TYPE", "TYPES", "UNBOUNDED", "UNCOMMITTED", "UNENCRYPTED", "UNION", "UNIQUE",
		"UNKNOWN", "UNLISTEN", "UNLOGGED", "UNTIL", "UPDATE", "USER", "USING", "VACUUM",
		"VALID", "VALIDATE", "VALIDATOR", "VALUE", "VALUES", "VARCHAR", "VARIADIC",
		"VARYING", "VERBOSE", "VERSION", "VIEW", "VIEWS", "VOLATILE", "WHEN", "WHERE",
		"WHITESPACE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRAPPER", "WRITE",
		"XML", "XMLATTRIBUTES", "XMLCONCAT", "XMLELEMENT", "XMLEXISTS", "XMLFOREST",
		"XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLROOT", "XMLSERIALIZE", "XMLTABLE",
		"YEAR", "YEARS", "YES", "ZONE",
	}
}

// getEnterpriseNumericFunctions returns comprehensive numeric functions.
func (p *EnhancedSQLInfoProvider) getEnterpriseNumericFunctions() []string {
	return []string{
		"ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEIL", "CEILING", "COS", "COT",
		"DEGREES", "EXP", "FLOOR", "LOG", "LOG10", "LOG2", "MOD", "PI", "POWER",
		"RADIANS", "RANDOM", "ROUND", "SIGN", "SIN", "SQRT", "TAN", "TRUNCATE",
		"GREATEST", "LEAST", "CBRT", "FACTORIAL", "GAMMA", "LGAMMA", "LN",
		"NEXTAFTER", "POW", "SETSEED", "XOR", "BIT_COUNT", "EVEN",
	}
}

// getEnterpriseStringFunctions returns comprehensive string functions.
func (p *EnhancedSQLInfoProvider) getEnterpriseStringFunctions() []string {
	return []string{
		"ASCII", "CHAR", "CHAR_LENGTH", "CHARACTER_LENGTH", "CHR", "CONCAT", "CONCAT_WS",
		"CONTAINS", "DIFFERENCE", "FORMAT", "INITCAP", "INSERT", "INSTR", "LCASE",
		"LEFT", "LENGTH", "LIKE", "LOCATE", "LOWER", "LPAD", "LTRIM", "MD5",
		"OCTET_LENGTH", "OVERLAY", "POSITION", "REPEAT", "REPLACE", "REVERSE",
		"RIGHT", "RPAD", "RTRIM", "SOUNDEX", "SPACE", "SPLIT_PART", "STRPOS",
		"SUBSTRING", "TRANSLATE", "TRIM", "UCASE", "UPPER", "BASE64", "FROM_BASE64",
		"TO_BASE64", "REGEXP_MATCHES", "REGEXP_REPLACE", "REGEXP_SPLIT_TO_ARRAY",
		"STRING_SPLIT", "STRING_TO_ARRAY", "ARRAY_TO_STRING", "EDITDIST3",
		"HAMMING", "JACCARD", "LEVENSHTEIN", "MISMATCHES", "NFC_NORMALIZE",
		"STRIP_ACCENTS", "UNICODE", "ORD", "PREFIX", "SUFFIX", "STARTS_WITH",
		"ENDS_WITH", "LIKE_ESCAPE", "NOT_LIKE_ESCAPE",
	}
}

// formatValue converts various value types to string representation.
func (p *EnhancedSQLInfoProvider) formatValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case []string:
		return fmt.Sprintf("[%s]", strings.Join(v, ","))
	case []int32:
		strs := make([]string, len(v))
		for i, n := range v {
			strs[i] = fmt.Sprintf("%d", n)
		}
		return fmt.Sprintf("[%s]", strings.Join(strs, ","))
	default:
		return fmt.Sprintf("%v", v)
	}
}
