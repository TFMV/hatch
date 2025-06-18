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

	codes := rec.Column(1).(*array.DenseUnion).TypeCodes()
	expected := []arrow.UnionTypeCode{0, 1, 2, 3, 4, 5, 0}
	require.Equal(t, expected, codes[:len(expected)])

	valUnion := rec.Column(1).(*array.DenseUnion)
	// row 0: string "hello"
	sv := valUnion.Field(0).(*array.String)
	assert.Equal(t, "hello", sv.Value(int(valUnion.ValueOffsets()[0])))

	// row 1: bool true
	bv := valUnion.Field(1).(*array.Boolean)
	assert.True(t, bv.Value(int(valUnion.ValueOffsets()[1])))

	// row 6: default branch -> empty string
	assert.Equal(t, "", sv.Value(int(valUnion.ValueOffsets()[6])))
}
