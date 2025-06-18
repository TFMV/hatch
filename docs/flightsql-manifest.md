# Flight SQL Implementation Coverage Manifest

## Metadata RPCs
- [x] GetCatalogs
- [x] GetSchemas
 - [x] GetTables
- [x] GetTableTypes
- [ ] GetColumns
- [x] GetPrimaryKeys
- [x] GetImportedKeys
- [x] GetExportedKeys
- [ ] GetCrossReference

## Execution RPCs
- [x] DoGet
- [~] DoPut
- [ ] DoExchange
- [x] CreatePreparedStatement
- [x] ClosePreparedStatement

## Prepared Statements
- [x] DoPutPreparedStatementUpdate
- [x] DoPutPreparedStatementQuery
- [x] Binding Parameters
- [x] Statement Execution Lifecycle

## SQL Info
- [~] GetSQLInfo
- [x] GetTypeInfo
