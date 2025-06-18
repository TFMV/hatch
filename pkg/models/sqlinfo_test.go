package models

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqlInfoResult_ToArrowRecord(t *testing.T) {
	allocator := memory.NewGoAllocator()

	infos := []SQLInfo{
		{InfoName: 1, Value: "hello"},
		{InfoName: 2, Value: true},
		{InfoName: 3, Value: int64(42)},
		{InfoName: 4, Value: int32(7)},
		{InfoName: 5, Value: []string{"a", "b"}},
		{InfoName: 6, Value: map[int32][]int32{1: {2, 3}}},
		{InfoName: 7, Value: 3.14}, // unrecognized type uses string branch
	}

	result := &SqlInfoResult{Info: infos}
	rec := result.ToArrowRecord(allocator)
	defer rec.Release()

	require.Equal(t, int64(len(infos)), rec.NumRows())
	require.True(t, rec.Schema().Equal(GetSqlInfoSchema()))

	valUnion := rec.Column(1).(*array.DenseUnion)

	// Check type codes for each row
	expectedCodes := []arrow.UnionTypeCode{0, 1, 2, 3, 4, 5, 0}
	for i := 0; i < len(expectedCodes); i++ {
		assert.Equal(t, expectedCodes[i], valUnion.TypeCode(i))
	}

	// row 0: string "hello"
	sv := valUnion.Field(0).(*array.String)
	offset0 := valUnion.ValueOffset(0)
	assert.Equal(t, "hello", sv.Value(int(offset0)))

	// row 1: bool true
	bv := valUnion.Field(1).(*array.Boolean)
	offset1 := valUnion.ValueOffset(1)
	assert.True(t, bv.Value(int(offset1)))

	// row 6: default branch -> empty string
	offset6 := valUnion.ValueOffset(6)
	assert.Equal(t, "", sv.Value(int(offset6)))
}
