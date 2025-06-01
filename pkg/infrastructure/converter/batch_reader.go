// Package converter provides type conversion between DuckDB and Apache Arrow.
package converter

import (
	"database/sql"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/TFMV/flight/pkg/errors"
)

const defaultBatchSize = 1024

// BatchReader reads SQL rows and converts them to Arrow record batches.
type BatchReader struct {
	refCount  atomic.Int64
	schema    *arrow.Schema
	rows      *sql.Rows
	record    arrow.Record
	builder   *array.RecordBuilder
	allocator memory.Allocator
	err       error
	rowDest   []interface{}
	logger    zerolog.Logger
	batchSize int
}

// NewBatchReader creates a new batch reader from SQL rows.
func NewBatchReader(allocator memory.Allocator, rows *sql.Rows, logger zerolog.Logger) (*BatchReader, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get column types")
	}

	tc := New(logger)
	fields := make([]arrow.Field, len(cols))
	rowDest := make([]interface{}, len(cols))

	for i, col := range cols {
		field, err := tc.GetArrowFieldFromColumn(col)
		if err != nil {
			rows.Close()
			return nil, errors.Wrapf(err, errors.CodeInternal, "failed to convert column %d", i)
		}
		fields[i] = field

		// Create destination based on field type and nullability
		rowDest[i] = createScanDest(field)
	}

	schema := arrow.NewSchema(fields, nil)

	r := &BatchReader{
		schema:    schema,
		rows:      rows,
		builder:   array.NewRecordBuilder(allocator, schema),
		allocator: allocator,
		rowDest:   rowDest,
		logger:    logger,
		batchSize: defaultBatchSize,
	}

	// Initialize refCount to 1
	r.refCount.Store(1)

	return r, nil
}

// NewBatchReaderWithSchema creates a new batch reader with a predefined schema.
func NewBatchReaderWithSchema(allocator memory.Allocator, schema *arrow.Schema, rows *sql.Rows, logger zerolog.Logger) (*BatchReader, error) {
	rowDest := make([]interface{}, schema.NumFields())

	for i, field := range schema.Fields() {
		rowDest[i] = createScanDest(field)
	}

	r := &BatchReader{
		schema:    schema,
		rows:      rows,
		builder:   array.NewRecordBuilder(allocator, schema),
		allocator: allocator,
		rowDest:   rowDest,
		logger:    logger,
		batchSize: defaultBatchSize,
	}

	// Initialize refCount to 1
	r.refCount.Store(1)

	return r, nil
}

// SetBatchSize sets the number of rows to read per batch.
func (r *BatchReader) SetBatchSize(size int) {
	if size > 0 {
		r.batchSize = size
	}
}

// Schema returns the Arrow schema.
func (r *BatchReader) Schema() *arrow.Schema {
	return r.schema
}

// Retain increases the reference count.
func (r *BatchReader) Retain() {
	r.refCount.Add(1)
}

// Release decreases the reference count and cleans up when it reaches 0.
func (r *BatchReader) Release() {
	if r.refCount.Add(-1) == 0 {
		r.cleanup()
	}
}

// cleanup releases all resources.
func (r *BatchReader) cleanup() {
	if r.rows != nil {
		r.rows.Close()
		r.rows = nil
	}
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
	if r.builder != nil {
		r.builder.Release()
		r.builder = nil
	}
}

// Record returns the current record batch.
func (r *BatchReader) Record() arrow.Record {
	return r.record
}

// Err returns any error that occurred during reading.
func (r *BatchReader) Err() error {
	return r.err
}

// Next reads the next batch of rows.
func (r *BatchReader) Next() bool {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}

	rows := 0
	start := time.Now()

	for rows < r.batchSize && r.rows.Next() {
		if err := r.rows.Scan(r.rowDest...); err != nil {
			r.err = errors.Wrap(err, errors.CodeQueryFailed, "failed to scan row")
			return false
		}

		for i, dest := range r.rowDest {
			if err := r.appendValue(i, dest); err != nil {
				r.err = errors.Wrapf(err, errors.CodeInternal, "failed to append value for column %d", i)
				return false
			}
		}

		rows++
	}

	if rows > 0 {
		r.record = r.builder.NewRecord()
		r.logger.Debug().
			Int("rows", rows).
			Dur("duration", time.Since(start)).
			Msg("Read batch")
	}

	if err := r.rows.Err(); err != nil {
		r.err = errors.Wrap(err, errors.CodeQueryFailed, "rows iteration error")
		return false
	}

	return rows > 0
}

// appendValue appends a scanned value to the appropriate builder.
func (r *BatchReader) appendValue(colIdx int, value interface{}) error {
	fb := r.builder.Field(colIdx)

	switch v := value.(type) {
	case *bool:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.BooleanBuilder).Append(*v)
		}
	case *sql.NullBool:
		if !v.Valid {
			fb.AppendNull()
		} else {
			fb.(*array.BooleanBuilder).Append(v.Bool)
		}

	case *int8:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Int8Builder).Append(*v)
		}
	case *uint8:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Uint8Builder).Append(*v)
		}
	case *sql.NullByte:
		if !v.Valid {
			fb.AppendNull()
		} else {
			fb.(*array.Uint8Builder).Append(v.Byte)
		}

	case *int16:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Int16Builder).Append(*v)
		}
	case *uint16:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Uint16Builder).Append(*v)
		}
	case *sql.NullInt16:
		if !v.Valid {
			fb.AppendNull()
		} else {
			fb.(*array.Int16Builder).Append(v.Int16)
		}

	case *int32:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Int32Builder).Append(*v)
		}
	case *uint32:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Uint32Builder).Append(*v)
		}
	case *sql.NullInt32:
		if !v.Valid {
			fb.AppendNull()
		} else {
			fb.(*array.Int32Builder).Append(v.Int32)
		}

	case *int64:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Int64Builder).Append(*v)
		}
	case *uint64:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Uint64Builder).Append(*v)
		}
	case *sql.NullInt64:
		if !v.Valid {
			fb.AppendNull()
		} else {
			fb.(*array.Int64Builder).Append(v.Int64)
		}

	case *float32:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Float32Builder).Append(*v)
		}
	case *float64:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.Float64Builder).Append(*v)
		}
	case *sql.NullFloat64:
		if !v.Valid {
			fb.AppendNull()
		} else {
			switch b := fb.(type) {
			case *array.Float64Builder:
				b.Append(v.Float64)
			case *array.Float32Builder:
				b.Append(float32(v.Float64))
			default:
				return errors.New(errors.CodeInternal, "unexpected builder type for float")
			}
		}

	case *string:
		if v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.StringBuilder).Append(*v)
		}
	case *sql.NullString:
		if !v.Valid {
			fb.AppendNull()
		} else {
			fb.(*array.StringBuilder).Append(v.String)
		}

	case *[]byte:
		if v == nil || *v == nil {
			fb.AppendNull()
		} else {
			fb.(*array.BinaryBuilder).Append(*v)
		}

	case *time.Time:
		if v == nil {
			fb.AppendNull()
		} else {
			if err := appendTimeValue(fb, *v); err != nil {
				return err
			}
		}
	case *sql.NullTime:
		if !v.Valid {
			fb.AppendNull()
		} else {
			if err := appendTimeValue(fb, v.Time); err != nil {
				return err
			}
		}

	case *interface{}:
		// Handle dynamic types
		if v == nil || *v == nil {
			fb.AppendNull()
		} else {
			return appendDynamicValue(fb, *v)
		}

	default:
		return errors.New(errors.CodeInternal, "unsupported scan type: "+reflect.TypeOf(value).String())
	}

	return nil
}

// createScanDest creates an appropriate scan destination based on the Arrow field type.
func createScanDest(field arrow.Field) interface{} {
	switch field.Type.ID() {
	case arrow.BOOL:
		if field.Nullable {
			return &sql.NullBool{}
		}
		return new(bool)

	case arrow.INT8:
		if field.Nullable {
			return &sql.NullByte{}
		}
		return new(int8)

	case arrow.UINT8:
		if field.Nullable {
			return &sql.NullByte{}
		}
		return new(uint8)

	case arrow.INT16:
		if field.Nullable {
			return &sql.NullInt16{}
		}
		return new(int16)

	case arrow.UINT16:
		if field.Nullable {
			// No sql.NullUint16, use pointer
			return new(*uint16)
		}
		return new(uint16)

	case arrow.INT32:
		if field.Nullable {
			return &sql.NullInt32{}
		}
		return new(int32)

	case arrow.UINT32:
		if field.Nullable {
			// No sql.NullUint32, use pointer
			return new(*uint32)
		}
		return new(uint32)

	case arrow.INT64:
		if field.Nullable {
			return &sql.NullInt64{}
		}
		return new(int64)

	case arrow.UINT64:
		if field.Nullable {
			// No sql.NullUint64, use pointer
			return new(*uint64)
		}
		return new(uint64)

	case arrow.FLOAT32:
		if field.Nullable {
			return &sql.NullFloat64{} // Will convert
		}
		return new(float32)

	case arrow.FLOAT64:
		if field.Nullable {
			return &sql.NullFloat64{}
		}
		return new(float64)

	case arrow.STRING:
		if field.Nullable {
			return &sql.NullString{}
		}
		return new(string)

	case arrow.BINARY:
		var b []byte
		return &b

	case arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64, arrow.TIMESTAMP:
		if field.Nullable {
			return &sql.NullTime{}
		}
		return new(time.Time)

	case arrow.DECIMAL, arrow.DECIMAL256:
		// Handle decimal as string for now
		if field.Nullable {
			return &sql.NullString{}
		}
		return new(string)

	default:
		// For unknown types, use interface{}
		return new(interface{})
	}
}

// appendTimeValue appends a time value to the appropriate builder.
func appendTimeValue(fb array.Builder, t time.Time) error {
	switch b := fb.(type) {
	case *array.Date32Builder:
		// Date32 is days since Unix epoch
		days := int32(t.Unix() / 86400)
		b.Append(arrow.Date32(days))

	case *array.Date64Builder:
		// Date64 is milliseconds since Unix epoch
		b.Append(arrow.Date64(t.UnixMilli()))

	case *array.Time32Builder:
		// Time32 seconds since midnight
		seconds := t.Hour()*3600 + t.Minute()*60 + t.Second()
		b.Append(arrow.Time32(seconds))

	case *array.Time64Builder:
		// Time64 microseconds since midnight
		micros := int64(t.Hour())*3600000000 + int64(t.Minute())*60000000 +
			int64(t.Second())*1000000 + int64(t.Nanosecond())/1000
		b.Append(arrow.Time64(micros))

	case *array.TimestampBuilder:
		// Timestamp microseconds since Unix epoch
		b.Append(arrow.Timestamp(t.UnixMicro()))

	default:
		return errors.New(errors.CodeInternal, "unexpected builder type for time value")
	}

	return nil
}

// appendDynamicValue appends a dynamically typed value.
func appendDynamicValue(fb array.Builder, value interface{}) error {
	if value == nil {
		fb.AppendNull()
		return nil
	}

	switch v := value.(type) {
	case bool:
		fb.(*array.BooleanBuilder).Append(v)
	case int64:
		fb.(*array.Int64Builder).Append(v)
	case float64:
		fb.(*array.Float64Builder).Append(v)
	case string:
		fb.(*array.StringBuilder).Append(v)
	case []byte:
		fb.(*array.BinaryBuilder).Append(v)
	case time.Time:
		return appendTimeValue(fb, v)
	default:
		// Try to convert to string
		fb.(*array.StringBuilder).Append(toString(v))
	}

	return nil
}

// toString converts a value to string.
func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	default:
		return ""
	}
}
