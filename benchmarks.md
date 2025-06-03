# Hatch Benchmarks

The following benchmarks were run on an Apple M2 Pro with Go 1.24.3.

> If you'd like to run the benchmarks, let me know so that I can provide the test data.

## Parquet Read Performance

| File | Operations/sec | Time per op | Memory/op | Allocs/op |
|------|---------------|-------------|-----------|-----------|
| Simple | 1,723 | 659µs | 2,178 B | 65 |
| Complex | 248 | 4.8ms | 2,190 B | 65 |
| SparkOnTime | 836 | 1.5ms | 2,177 B | 65 |
| SparkStore | 1,131 | 1.1ms | 2,178 B | 65 |
| UserData | 790 | 1.4ms | 2,204 B | 68 |
| LineItem | 146 | 8.3ms | 2,267 B | 68 |
| SortedZstd | 93 | 12.7ms | 2,237 B | 68 |

## Parquet Write Performance

| File | Operations/sec | Time per op | Memory/op | Allocs/op |
|------|---------------|-------------|-----------|-----------|
| Simple | 1,374 | 810µs | 1,284 B | 26 |
| Complex | 134 | 9.3ms | 1,287 B | 25 |
| SparkOnTime | 234 | 5.1ms | 1,310 B | 25 |
| SparkStore | 436 | 2.9ms | 1,321 B | 26 |
| UserData | 465 | 2.5ms | 1,294 B | 26 |

## Flight Stream Performance

| File | Operations/sec | Time per op | Memory/op | Allocs/op | Throughput |
|------|---------------|-------------|-----------|-----------|------------|
| SparkOnTime | 169 | 6.6ms | 152,314 B | 2,955 | 24.5 MB/s |
| SparkStore | 330 | 3.7ms | 68,174 B | 1,145 | 21.4 MB/s |
| UserData | 345 | 3.5ms | 37,258 B | 717 | 12.3 MB/s |

Note: Throughput is calculated as (Memory per operation × Operations per second) converted to MB/s.

## Compression Format Performance

| Format | Operations/sec | Time per op | Memory/op | Allocs/op |
|--------|---------------|-------------|-----------|-----------|
| Gzip | 100 | 10.4ms | 1,107 B | 34 |
| Zstd | 1,867 | 566µs | 1,064 B | 34 |
| Uncompressed | 2,258 | 534µs | 1,059 B | 34 |

## Data Type Performance

| Type | Operations/sec | Time per op | Memory/op | Allocs/op |
|------|---------------|-------------|-----------|-----------|
| Timestamps | 3,844 | 285µs | 1,060 B | 34 |
| TimeTZ | 4,244 | 276µs | 1,059 B | 34 |
| Unsigned | 3,188 | 359µs | 1,061 B | 34 |
| Struct | 3,772 | 308µs | 1,061 B | 34 |
| Map | 331 | 3.5ms | 1,080 B | 34 |
| List | 211 | 5.7ms | 1,088 B | 34 |
| Decimal | 2,766 | 385µs | 1,061 B | 34 |

## Concurrent Access Performance

| File | Concurrency | Operations/sec | Time per op | Memory/op | Allocs/op |
|------|-------------|---------------|-------------|-----------|-----------|
| Simple | 2 | 3,069 | 370µs | 2,462 B | 78 |
| Simple | 4 | 5,932 | 215µs | 2,448 B | 78 |
| Simple | 8 | 4,308 | 233µs | 2,469 B | 78 |
| Simple | 16 | 5,992 | 189µs | 2,443 B | 78 |
| Complex | 2 | 394 | 3.2ms | 2,469 B | 78 |
| Complex | 4 | 517 | 2.9ms | 2,458 B | 78 |
| Complex | 8 | 660 | 1.7ms | 2,454 B | 78 |
| Complex | 16 | 682 | 1.7ms | 2,454 B | 78 |
| SparkOnTime | 2 | 1,476 | 797µs | 2,466 B | 78 |
| SparkOnTime | 4 | 2,343 | 507µs | 2,468 B | 78 |
| SparkOnTime | 8 | 2,059 | 517µs | 2,464 B | 78 |
| SparkOnTime | 16 | 2,401 | 468µs | 2,465 B | 78 |

## Large Data Movement Performance

| Batch Size | Operations/sec | Time per op | Memory/op | Allocs/op |
|------------|---------------|-------------|-----------|-----------|
| 10,000 | 100 | 10.0ms | 730 KB | 131,986 |
| 50,000 | 27 | 43.1ms | 3.65 MB | 659,987 |
| 100,000 | 14 | 84.2ms | 7.30 MB | 1,320,011 |
| 500,000 | 3 | 365.9ms | 36.5 MB | 6,600,098 |
| 1,000,000 | 2 | 722.2ms | 73.0 MB | 13,200,085 |

## Flight Statement Performance

| Query Type | Operations/sec | Time per op | Memory/op | Allocs/op |
|------------|---------------|-------------|-----------|-----------|
| Simple SELECT | 12,942 | 90.9µs | 12.2 KB | 144 |
| Large Table LIMIT | 1,240 | 906.6µs | 198.8 KB | 8,043 |
| Filtered Query | 962 | 1.1ms | 235.5 KB | 8,298 |
| Group By Query | 134 | 8.6ms | 15.7 KB | 192 |

## Memory Pool Performance

| Operation | Operations/sec | Time per op | Memory/op | Allocs/op |
|-----------|---------------|-------------|-----------|-----------|
| RecordBuilder Get/Put | 143,348,066 | 8.4ns | 0 B | 0 |
| RecordBuilder Build | 716,593 | 1.5µs | 2.6 KB | 31 |
| ByteBuffer 64B | 34,970,086 | 34.5ns | 24 B | 1 |
| ByteBuffer 1KB | 35,736,753 | 34.2ns | 24 B | 1 |
| ByteBuffer 4KB | 32,696,396 | 36.2ns | 24 B | 1 |
| ByteBuffer 16KB | 31,844,421 | 37.5ns | 24 B | 1 |

## Memory Usage Patterns

| Dataset Size | Operations/sec | Time per op | Memory/op | Allocs/op |
|--------------|---------------|-------------|-----------|-----------|
| 1,000 rows | 20,221 | 58.2µs | 88.8 KB | 82 |
| 10,000 rows | 2,026 | 585.3µs | 1.34 MB | 119 |
| 100,000 rows | 295 | 4.0ms | 9.85 MB | 147 |
