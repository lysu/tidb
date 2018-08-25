package chunk

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"testing"
)

type opType int

const (
	gt opType = iota
	mod
)

func BenchmarkByFieldMod3Int64Column(b *testing.B) {
	inputSize := 1024
	ic := setupInt64Chunk(inputSize)
	oc := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}}, inputSize)
	for i := 0; i < b.N; i++ {
		copyFilterInt64ViaRow(ic, oc, mod)
	}
}

func BenchmarkByColumnMod3Int64Column(b *testing.B) {
	inputSize := 1024
	ic := setupInt64Chunk(inputSize)
	oc := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}}, inputSize)
	mask := make([]bool, inputSize)
	for i := 0; i < b.N; i++ {
		copyFilterInt64ViaCol(ic, oc, mask, mod)
	}
}

func BenchmarkByFieldGt3Int64Column(b *testing.B) {
	inputSize := 1024
	ic := setupInt64Chunk(inputSize)
	oc := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}}, inputSize)
	for i := 0; i < b.N; i++ {
		copyFilterInt64ViaRow(ic, oc, gt)
	}
}

func BenchmarkByColumnGt3Int64Column(b *testing.B) {
	inputSize := 1024
	ic := setupInt64Chunk(inputSize)
	oc := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}}, inputSize)
	mask := make([]bool, inputSize)
	for i := 0; i < b.N; i++ {
		copyFilterInt64ViaCol(ic, oc, mask, gt)
	}
}

func BenchmarkByFieldMod3StrColumn(b *testing.B) {
	inputSize := 1024
	ic := setupStrChunk(inputSize, mod)
	oc := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}}, inputSize)
	for i := 0; i < b.N; i++ {
		copyFilterStrViaRow(ic, oc)
	}
}

func BenchmarkByColumnMod3StrColumn(b *testing.B) {
	inputSize := 1024
	ic := setupStrChunk(inputSize, mod)
	oc := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}}, inputSize)
	mask := make([]bool, inputSize)
	for i := 0; i < b.N; i++ {
		copyFilterStrViaCol(ic, oc, mask)
	}
}

func BenchmarkByFieldGt3StrColumn(b *testing.B) {
	inputSize := 1024
	ic := setupStrChunk(inputSize, gt)
	oc := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}}, inputSize)
	for i := 0; i < b.N; i++ {
		copyFilterStrViaRow(ic, oc)
	}
}

func BenchmarkByColumnGt3StrColumn(b *testing.B) {
	inputSize := 1024
	ic := setupStrChunk(inputSize, gt)
	oc := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}}, inputSize)
	mask := make([]bool, inputSize)
	for i := 0; i < b.N; i++ {
		copyFilterStrViaCol(ic, oc, mask)
	}
}

func setupInt64Chunk(inputSize int) *Chunk {
	chk := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}, {Tp: mysql.TypeLong}}, inputSize)
	for i := 0; i < inputSize; i++ {
		chk.AppendInt64(0, int64(i))
		chk.AppendInt64(1, int64(i))
		chk.AppendInt64(2, int64(i))
	}
	return chk
}

func setupStrChunk(inputSize int, op opType) *Chunk {
	chk := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}, {Tp: mysql.TypeVarchar}}, inputSize)
	for i := 0; i < inputSize; i++ {
		switch op {
		case mod:
			if i%3 == 0 {
				chk.AppendString(0, "abc")
				chk.AppendString(1, "abc")
				chk.AppendString(2, "abc")
			} else {
				chk.AppendString(0, "ac")
				chk.AppendString(1, "a")
				chk.AppendString(2, "abdc")
			}
		case gt:
			if i > 512 {
				chk.AppendString(0, "abc")
				chk.AppendString(1, "abc")
				chk.AppendString(2, "abc")
			} else {
				chk.AppendString(0, "ac")
				chk.AppendString(1, "a")
				chk.AppendString(2, "abdc")
			}
		}

	}
	return chk
}

func copyFilterInt64ViaRow(ic, oc *Chunk, op opType) {
	oc.Reset()
	iter := NewIterator4Chunk(ic)
	for r := iter.Begin(); r != iter.End(); r = iter.Next() {
		switch op {
		case gt:
			if r.GetInt64(0) > 512 {
				oc.AppendRow(r)
			}
		case mod:
			if r.GetInt64(0)%3 == 0 {
				oc.AppendRow(r)
			}
		}
	}
}

func copyFilterInt64ViaCol(ic, oc *Chunk, mask []bool, op opType) {
	oc.Reset()
	mask = mask[0:]
	iter := NewIterator4Chunk(ic)
	idx := 0
	for r := iter.Begin(); r != iter.End(); r = iter.Next() {
		switch op {
		case gt:
			if r.GetInt64(0) > 512 {
				mask[idx] = true
			}
			idx++
		case mod:
			if r.GetInt64(0)%3 == 0 {
				mask[idx] = true
			}
			idx++
		}
	}
	oc.FilterThenAppend(mask, ic)
}

func copyFilterStrViaRow(ic, oc *Chunk) {
	oc.Reset()
	iter := NewIterator4Chunk(ic)
	for r := iter.Begin(); r != iter.End(); r = iter.Next() {
		if r.GetString(0) == "abc" {
			oc.AppendRow(r)
		}
	}
}

func copyFilterStrViaCol(ic, oc *Chunk, mask []bool) {
	oc.Reset()
	mask = mask[0:]
	iter := NewIterator4Chunk(ic)
	idx := 0
	for r := iter.Begin(); r != iter.End(); r = iter.Next() {
		if r.GetString(0) == "abc" {
			mask[idx] = true
		}
		idx++
	}
	oc.FilterThenAppend(mask, ic)
}

func TestFilterFixedType(t *testing.T) {
	ic := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeLong}}, 10)
	ic.AppendInt64(0, 1)
	ic.AppendNull(0)
	ic.AppendInt64(0, 1)

	oc1 := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeLong}}, 10)
	oc1.Append(ic, 0, ic.NumRows())

	mask := make([]bool, 3)
	mask[0] = true
	mask[1] = true
	mask[2] = true
	oc2 := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeLong}}, 10)
	oc2.FilterThenAppend(mask, ic)

}

func TestFilterVarType(t *testing.T) {
	ic := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeVarchar}}, 10)
	ic.AppendString(0, "abc")
	ic.AppendNull(0)
	ic.AppendString(0, "c")

	oc1 := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeVarchar}}, 10)
	oc1.Append(ic, 0, ic.NumRows())

	mask := make([]bool, 3)
	mask[0] = true
	mask[1] = true
	mask[2] = true
	oc2 := NewChunkWithCapacity([]*types.FieldType{{Tp: mysql.TypeVarchar}}, 10)
	oc2.FilterThenAppend(mask, ic)
}
