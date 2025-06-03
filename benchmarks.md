# Hatch Performance Benchmarks

## Summary of Changes

- Added new benchmarks for Flight streaming, compression formats, and data types
- Added concurrent access benchmarks with varying concurrency levels
- Added large data movement benchmarks with different batch sizes
- Added memory usage benchmarks
- Added parallel query execution benchmarks
- Added record builder and byte buffer pool benchmarks

## Parquet Read Performance

| File | Operations/sec | ns/op | B/op | allocs/op |
|------|---------------|-------|------|-----------|
| Simple | 1,720 | 665,160 | 2,186 | 65 |
| Complex | 241 | 5,028,312 | 2,184 | 65 |
| SparkOnTime | 805 | 1,490,163 | 2,187 | 65 |
| SparkStore | 1,102 | 1,083,774 | 2,179 | 65 |
| UserData | 769 | 1,432,029 | 2,197 | 68 |
| LineItem | 144 | 8,244,084 | 2,239 | 68 |
| SortedZstd | 94 | 12,738,902 | 2,241 | 68 |

## Parquet Write Performance

| File | Operations/sec | ns/op | B/op | allocs/op |
|------|---------------|-------|------|-----------|
| Simple | 1,339 | 890,881 | 1,283 | 26 |
| Complex | 123 | 10,117,129 | 1,279 | 25 |
| SparkOnTime | 202 | 8,045,878 | 1,303 | 25 |
| SparkStore | 331 | 3,090,516 | 1,314 | 25 |
| UserData | 451 | 2,784,577 | 1,286 | 26 |

## Flight Streaming Performance

| File | Operations/sec | ns/op | B/op | allocs/op |
|------|---------------|-------|------|-----------|
| SparkOnTime | 162 | 7,157,376 | 186,804 | 3,377 |
| SparkStore | 285 | 4,375,162 | 68,184 | 1,146 |
| UserData | 337 | 3,424,743 | 37,299 | 718 |

## Compression Format Performance

| Format | Operations/sec | ns/op | B/op | allocs/op |
|--------|---------------|-------|------|-----------|
| Gzip | 98 | 10,728,426 | 1,129 | 34 |
| Zstd | 2,209 | 488,711 | 1,064 | 34 |
| Uncompressed | 2,366 | 507,160 | 1,058 | 34 |

## Data Type Performance

| Type | Operations/sec | ns/op | B/op | allocs/op |
|------|---------------|-------|------|-----------|
| Timestamps | 3,861 | 288,313 | 1,061 | 34 |
| TimeTZ | 3,667 | 283,369 | 1,061 | 34 |
| Unsigned | 3,135 | 370,148 | 1,060 | 34 |
| Struct | 3,555 | 313,240 | 1,060 | 34 |
| Map | 354 | 3,158,154 | 1,068 | 34 |
| List | 232 | 5,322,628 | 1,081 | 34 |
| Decimal | 3,076 | 378,196 | 1,062 | 34 |

## Concurrent Access Performance

| File | Concurrency | Operations/sec | ns/op | B/op | allocs/op |
|------|-------------|---------------|-------|------|-----------|
| Simple | 2 | 3,218 | 362,202 | 2,460 | 78 |
| Simple | 4 | 5,906 | 221,511 | 2,460 | 78 |
| Simple | 8 | 5,521 | 199,619 | 2,456 | 78 |
| Simple | 16 | 6,852 | 194,442 | 2,444 | 78 |
| Complex | 2 | 416 | 2,818,259 | 2,469 | 78 |
| Complex | 4 | 626 | 1,808,135 | 2,450 | 78 |
| Complex | 8 | 634 | 1,722,959 | 2,460 | 78 |
| Complex | 16 | 708 | 1,712,477 | 2,454 | 78 |
| SparkOnTime | 2 | 1,356 | 831,603 | 2,479 | 78 |
| SparkOnTime | 4 | 2,113 | 510,203 | 2,467 | 78 |
| SparkOnTime | 8 | 2,043 | 503,564 | 2,468 | 78 |
| SparkOnTime | 16 | 2,444 | 496,526 | 2,466 | 78 |

## Large Data Movement Performance

| Batch Size | Operations/sec | ns/op | bytes/op | B/op | allocs/op |
|------------|---------------|-------|----------|------|-----------|
| 10,000 | 129 | 9,510,845 | 730,000 | 4,158,347 | 132,402 |
| 50,000 | 26 | 44,592,870 | 3,650,000 | 20,888,152 | 661,689 |
| 100,000 | 12 | 89,290,201 | 7,300,000 | 41,820,194 | 1,323,423 |
| 500,000 | 3 | 402,454,986 | 36,500,000 | 209,149,506 | 6,617,535 |
| 1,000,000 | 2 | 788,686,354 | 73,000,000 | 418,323,792 | 13,235,275 |

## Memory Usage Performance

| Size | Operations/sec | ns/op | B/op | allocs/op |
|------|---------------|-------|------|-----------|
| 1,000 | 20,382 | 59,178 | 88,758 | 82 |
| 10,000 | 2,079 | 599,845 | 1,338,341 | 119 |
| 100,000 | 292 | 4,050,085 | 9,849,864 | 147 |

## Pool Performance

### Record Builder Pool

| Operation | Operations/sec | ns/op | B/op | allocs/op |
|-----------|---------------|-------|------|-----------|
| Get/Put | 136,580,787 | 8.663 | 0 | 0 |
| BuildRecord | 709,884 | 1,603 | 2,619 | 31 |

### Byte Buffer Pool

| Size | Operations/sec | ns/op | B/op | allocs/op |
|------|---------------|-------|------|-----------|
| 64 | 33,380,350 | 35.50 | 24 | 1 |
| 1,024 | 35,695,074 | 33.77 | 24 | 1 |
| 4,096 | 33,108,889 | 35.83 | 24 | 1 |
| 16,384 | 32,737,688 | 36.77 | 24 | 1 |

## Key Observations

1. **Compression Performance**: Zstd shows excellent performance, nearly matching uncompressed data while providing compression benefits.
2. **Concurrent Scaling**: Simple operations scale well with concurrency up to 16 threads, while complex operations show diminishing returns after 4 threads.
3. **Memory Usage**: Linear scaling of memory usage with data size, with reasonable allocation counts.
4. **Pool Performance**: Record builder and byte buffer pools show excellent performance with minimal allocations.
5. **Large Data Movement**: Shows expected linear scaling with batch size, with memory usage growing proportionally.
6. **Flight Streaming**: Higher allocation counts compared to direct parquet operations, indicating potential optimization opportunities in the streaming layer.

## Environment

- OS: darwin
- Architecture: arm64
- CPU: Apple M2 Pro
- Go Version: go1.24.3
