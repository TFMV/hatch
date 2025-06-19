# Porter Models Package

The models package provides core data structures and schemas used throughout the Flight SQL server, including query handling, metadata management, and Arrow schema definitions.

## Design Overview

### Core Components

1. **Query Models**

   ```go
   type QueryRequest struct {
       Query         string
       TransactionID string
       Parameters    []interface{}
       MaxRows       int64
       Timeout       time.Duration
       Properties    map[string]interface{}
   }

   type QueryResult struct {
       Schema        *arrow.Schema
       Records       <-chan arrow.Record
       TotalRows     int64
       ExecutionTime time.Duration
       Metadata      map[string]interface{}
   }
   ```

2. **Transaction Models**

   ```go
   type Transaction struct {
       ID             string
       IsolationLevel IsolationLevel
       ReadOnly       bool
       StartedAt      time.Time
       LastActivityAt time.Time
       State          TransactionState
   }
   ```

3. **Metadata Models**

   ```go
   type Catalog struct {
       Name        string
       Description string
       Properties  map[string]interface{}
   }

   type Schema struct {
       CatalogName string
       Name        string
       Owner       string
       Description string
       Properties  map[string]interface{}
   }

   type Table struct {
       CatalogName string
       SchemaName  string
       Name        string
       Type        string
       Description string
       Owner       string
       CreatedAt   *time.Time
       UpdatedAt   *time.Time
       RowCount    *int64
       Properties  map[string]interface{}
   }
   ```

## Implementation Details

### Query Handling

1. **Query Requests**
   - SQL query string
   - Transaction context
   - Parameter binding
   - Execution limits
   - Custom properties

2. **Query Results**
   - Arrow schema
   - Record streaming
   - Execution metrics
   - Result metadata

3. **Prepared Statements**
   - Statement handle
   - Parameter schema
   - Result schema
   - Usage tracking

### Transaction Management

1. **Transaction States**

   ```go
   const (
       TransactionStateActive      = "ACTIVE"
       TransactionStateCommitting  = "COMMITTING"
       TransactionStateRollingBack = "ROLLING_BACK"
       TransactionStateCommitted   = "COMMITTED"
       TransactionStateRolledBack  = "ROLLED_BACK"
   )
   ```

2. **Isolation Levels**

   ```go
   const (
       IsolationLevelDefault         = ""
       IsolationLevelReadUncommitted = "READ_UNCOMMITTED"
       IsolationLevelReadCommitted   = "READ_COMMITTED"
       IsolationLevelRepeatableRead  = "REPEATABLE_READ"
       IsolationLevelSerializable    = "SERIALIZABLE"
   )
   ```

### Metadata Management

1. **Database Objects**
   - Catalogs
   - Schemas
   - Tables
   - Columns
   - Keys

2. **Type Information**
   - XDBC types
   - SQL info
   - Type mappings
   - Type properties

3. **Statistics**
   - Table statistics
   - Index statistics
   - Column statistics
   - Usage metrics

### Arrow Schema Integration

1. **Schema Definitions**

   ```go
   func GetCatalogsSchema() *arrow.Schema
   func GetDBSchemasSchema() *arrow.Schema
   func GetTablesSchema(includeSchema bool) *arrow.Schema
   func GetTableTypesSchema() *arrow.Schema
   ```

2. **Result Conversion**

   ```go
   type XdbcTypeInfoResult struct {
       Types []XdbcTypeInfo
   }

   func (x *XdbcTypeInfoResult) ToArrowRecord(allocator memory.Allocator) arrow.Record
   ```

## Usage

### Query Execution

```go
// Create query request
request := models.QueryRequest{
    Query:         "SELECT * FROM users WHERE age > ?",
    Parameters:    []interface{}{18},
    MaxRows:       1000,
    Timeout:       30 * time.Second,
    Properties:    map[string]interface{}{"cache": true},
}

// Handle query result
result := models.QueryResult{
    Schema:        schema,
    Records:       records,
    TotalRows:     100,
    ExecutionTime: 50 * time.Millisecond,
    Metadata:      map[string]interface{}{"cached": true},
}
```

### Transaction Management

```go
// Create transaction
tx := models.Transaction{
    ID:             "tx-123",
    IsolationLevel: models.IsolationLevelReadCommitted,
    ReadOnly:       false,
    StartedAt:      time.Now(),
    State:          models.TransactionStateActive,
}

// Update transaction state
tx.State = models.TransactionStateCommitting
tx.LastActivityAt = time.Now()
```

### Metadata Access

```go
// Get table information
table := models.Table{
    CatalogName: "main",
    SchemaName:  "public",
    Name:        "users",
    Type:        "TABLE",
    Description: "User accounts",
    RowCount:    &rowCount,
}

// Get column information
column := models.Column{
    CatalogName:     "main",
    SchemaName:      "public",
    TableName:       "users",
    Name:            "email",
    DataType:        "VARCHAR",
    IsNullable:      false,
    CharMaxLength:   sql.NullInt64{Int64: 255, Valid: true},
}
```

## Best Practices

1. **Query Handling**
   - Use parameterized queries
   - Set appropriate timeouts
   - Handle large result sets
   - Monitor execution metrics

2. **Transaction Management**
   - Choose appropriate isolation
   - Handle transaction states
   - Monitor transaction duration
   - Implement proper cleanup

3. **Metadata Usage**
   - Cache metadata when possible
   - Handle null values properly
   - Validate object references
   - Track metadata changes

4. **Arrow Integration**
   - Reuse schema definitions
   - Handle memory allocation
   - Manage record streaming
   - Monitor conversion overhead

## Testing

The package includes comprehensive tests:

1. **Query Tests**
   - Request validation
   - Result handling
   - Parameter binding
   - Error cases

2. **Transaction Tests**
   - State transitions
   - Isolation levels
   - Concurrent access
   - Error handling

3. **Metadata Tests**
   - Object validation
   - Type conversion
   - Null handling
   - Reference integrity

## Performance Considerations

1. **Memory Usage**
   - Schema caching
   - Record batching
   - Memory allocation
   - Resource cleanup

2. **CPU Impact**
   - Schema validation
   - Type conversion
   - State management
   - Metadata processing

3. **Concurrency**
   - State synchronization
   - Resource sharing
   - Transaction isolation
   - Cache consistency

## Integration Examples

### Custom Query Handler

```go
type CustomQueryHandler struct {
    models.QueryRequest
    // Additional fields
}

func (h *CustomQueryHandler) Execute(ctx context.Context) (*models.QueryResult, error) {
    // Custom execution logic
    return &models.QueryResult{
        Schema: h.Schema,
        Records: h.Records,
    }, nil
}
```

### Metadata Cache

```go
type MetadataCache struct {
    catalogs map[string]*models.Catalog
    schemas  map[string]*models.Schema
    tables   map[string]*models.Table
    mu       sync.RWMutex
}

func (c *MetadataCache) GetTable(catalog, schema, table string) (*models.Table, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    key := fmt.Sprintf("%s.%s.%s", catalog, schema, table)
    return c.tables[key], nil
}
```

### Arrow Record Processor

```go
type RecordProcessor struct {
    schema *arrow.Schema
    allocator memory.Allocator
}

func (p *RecordProcessor) ProcessRecords(records <-chan arrow.Record) <-chan arrow.Record {
    out := make(chan arrow.Record)
    go func() {
        defer close(out)
        for record := range records {
            // Process record
            out <- record
        }
    }()
    return out
}
```
