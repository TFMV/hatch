# Flight SQL Implementation Coverage Manifest

## Metadata RPCs
- [x] GetCatalogs
- [x] GetSchemas
 - [x] GetTables
- [x] GetTableTypes
- [x] GetColumns
- [x] GetPrimaryKeys
- [x] GetImportedKeys
- [x] GetExportedKeys
- [x] GetCrossReference

## Execution RPCs
- [x] DoGet
- [x] DoPut
 - [~] DoExchange
- [x] CreatePreparedStatement
- [x] ClosePreparedStatement

## Prepared Statements
- [x] DoPutPreparedStatementUpdate
- [x] DoPutPreparedStatementQuery
- [x] Binding Parameters
- [x] Statement Execution Lifecycle

## SQL Info
- [x] GetSQLInfo
- [x] GetTypeInfo

## JDBC Compatibility Checklist
- [x] GetCatalogs
- [x] GetSchemas
- [x] GetTables
- [x] GetTableTypes
- [x] GetColumns
- [x] GetTypeInfo
- [x] GetSQLInfo
- [x] CreatePreparedStatement
- [x] ClosePreparedStatement
