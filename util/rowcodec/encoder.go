// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rowcodec

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"math"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// Encoder is used to encode a row.
type Encoder struct {
	row
	tempColIDs []int64
	values     []types.Datum
	tempData   []byte
}

func (encoder *Encoder) reset() {
	encoder.large = false
	encoder.numNotNullCols = 0
	encoder.numNullCols = 0
	encoder.data = encoder.data[:0]
	encoder.tempColIDs = encoder.tempColIDs[:0]
	encoder.values = encoder.values[:0]
}

func (encoder *Encoder) addColumn(colID int64, d types.Datum) {
	if colID > 255 {
		encoder.large = true
	}
	if d.IsNull() {
		encoder.numNullCols++
	} else {
		encoder.numNotNullCols++
	}
	encoder.tempColIDs = append(encoder.tempColIDs, colID)
	encoder.values = append(encoder.values, d)
}

// Encode encodes a row from a datums slice.
func (encoder *Encoder) Encode(sc *stmtctx.StatementContext, colIDs []int64, values []types.Datum, buf []byte) ([]byte, error) {
	encoder.reset()
	for i, colID := range colIDs {
		encoder.addColumn(colID, values[i])
	}
	return encoder.build(sc, buf[:0])
}

// EncodeFromOldRow encodes a row from an old-format row.
func (encoder *Encoder) EncodeFromOldRow(sc *stmtctx.StatementContext, oldRow, buf []byte) ([]byte, error) {
	encoder.reset()
	for len(oldRow) > 1 {
		var d types.Datum
		var err error
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		colID := d.GetInt64()
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		encoder.addColumn(colID, d)
	}
	return encoder.build(sc, buf[:0])
}

func (encoder *Encoder) build(sc *stmtctx.StatementContext, buf []byte) ([]byte, error) {
	numCols, notNullIdx := encoder.separateNilAndSort()
	err := encoder.prepareColData(sc, numCols, notNullIdx)
	if err != nil {
		return nil, err
	}
	buf = encoder.encodeBytes(buf)
	return buf, nil
}

func (encoder *Encoder) separateNilAndSort() (numCols, notNullIdx int) {
	r := &encoder.row
	numCols = len(encoder.tempColIDs)
	nullIdx := numCols - int(r.numNullCols)
	notNullIdx = 0
	if r.large {
		encoder.initColIDs32()
		encoder.initOffsets32()
	} else {
		encoder.initColIDs()
		encoder.initOffsets()
	}
	for i, colID := range encoder.tempColIDs {
		if encoder.values[i].IsNull() {
			if r.large {
				r.colIDs32[nullIdx] = uint32(colID)
			} else {
				r.colIDs[nullIdx] = byte(colID)
			}
			nullIdx++
		} else {
			if r.large {
				r.colIDs32[notNullIdx] = uint32(colID)
			} else {
				r.colIDs[notNullIdx] = byte(colID)
			}
			encoder.values[notNullIdx] = encoder.values[i]
			notNullIdx++
		}
	}
	if r.large {
		largeNotNullSorter := (*largeNotNullSorter)(encoder)
		sort.Sort(largeNotNullSorter)
		if r.numNullCols > 0 {
			largeNullSorter := (*largeNullSorter)(encoder)
			sort.Sort(largeNullSorter)
		}
	} else {
		smallNotNullSorter := (*smallNotNullSorter)(encoder)
		sort.Sort(smallNotNullSorter)
		if r.numNullCols > 0 {
			smallNullSorter := (*smallNullSorter)(encoder)
			sort.Sort(smallNullSorter)
		}
	}
	return
}

func (encoder *Encoder) prepareColData(sc *stmtctx.StatementContext, numCols, notNullIdx int) error {
	r := &encoder.row
	encoder.initValFlags()
	for i := 0; i < notNullIdx; i++ {
		d := encoder.values[i]
		err := encoder.encodeDatum(sc, d)
		if err != nil {
			return err
		}
		flag, err := type2Flag(d.Kind())
		if err != nil {
			return err
		}
		r.valFlags[i] = flag
		// handle convert to large
		if len(r.data) > math.MaxUint16 && !r.large {
			encoder.initColIDs32()
			for j := 0; j < numCols; j++ {
				r.colIDs32[j] = uint32(r.colIDs[j])
			}
			encoder.initOffsets32()
			for j := 0; j <= i; j++ {
				r.offsets32[j] = uint32(r.offsets[j])
			}
			r.large = true
		}
		if r.large {
			r.offsets32[i] = uint32(len(r.data))
		} else {
			r.offsets[i] = uint16(len(r.data))
		}
	}
	// handle convert to large
	if !r.large {
		if len(r.data) >= math.MaxUint16 {
			r.large = true
			encoder.initColIDs32()
			for i, val := range r.colIDs {
				r.colIDs32[i] = uint32(val)
			}
		} else {
			encoder.initOffsets()
			for i, val := range r.offsets32 {
				r.offsets[i] = uint16(val)
			}
		}
	}
	return nil
}

func (encoder *Encoder) encodeDatum(sc *stmtctx.StatementContext, d types.Datum) (err error) {
	r := &encoder.row
	switch d.Kind() {
	case types.KindInt64:
		r.data = encodeInt(r.data, d.GetInt64())
	case types.KindUint64:
		r.data = encodeUint(r.data, d.GetUint64())
	case types.KindString, types.KindBytes:
		r.data = append(r.data, d.GetBytes()...)

	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		t := d.GetMysqlTime()
		if t.Type == mysql.TypeTimestamp && sc != nil && sc.TimeZone != time.UTC {
			err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
			if err != nil {
				return
			}
		}
		var v uint64
		v, err = t.ToPackedUint()
		if err != nil {
			return
		}
		r.data = encodeUint(r.data, v)
	case types.KindMysqlDuration:
		r.data = encodeInt(r.data, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlEnum:
		r.data = encodeUint(r.data, d.GetMysqlEnum().Value)
	case types.KindMysqlSet:
		r.data = encodeUint(r.data, d.GetMysqlSet().Value)
	case types.KindBinaryLiteral, types.KindMysqlBit:
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		var val uint64
		val, err = d.GetBinaryLiteral().ToInt(sc)
		if err != nil {
			return
		}
		r.data = encodeUint(r.data, val)
	case types.KindFloat32, types.KindFloat64:
		r.data = codec.EncodeFloat(r.data, d.GetFloat64())
	case types.KindMysqlDecimal:
		r.data, err = codec.EncodeDecimal(r.data, d.GetMysqlDecimal(), d.Length(), d.Frac())
		if sc != nil {
			if terror.ErrorEqual(err, types.ErrTruncated) {
				err = sc.HandleTruncate(err)
			} else if terror.ErrorEqual(err, types.ErrOverflow) {
				err = sc.HandleOverflow(err, err)
			}
		}
	case types.KindMysqlJSON:
		j := d.GetMysqlJSON()
		r.data = append(r.data, j.TypeCode)
		r.data = append(r.data, j.Value...)
	case types.KindNull:
	case types.KindMinNotNull:
	case types.KindMaxValue:
	default:
		err = errors.Errorf("unsupport encode type %d", d.Kind())
	}
	return
}

func type2Flag(tp byte) (flag byte, err error) {
	switch tp {
	case types.KindInt64:
		flag = IntFlag
	case types.KindUint64:
		flag = UintFlag
	case types.KindString, types.KindBytes:
		flag = BytesFlag
	case types.KindMysqlTime:
		flag = UintFlag
	case types.KindMysqlDuration:
		flag = IntFlag
	case types.KindMysqlEnum:
		flag = UintFlag
	case types.KindMysqlSet:
		flag = UintFlag
	case types.KindBinaryLiteral, types.KindMysqlBit:
		flag = UintFlag
	case types.KindFloat32, types.KindFloat64:
		flag = FloatFlag
	case types.KindMysqlDecimal:
		flag = DecimalFlag
	case types.KindMysqlJSON:
		flag = JSONFlag
	case types.KindNull:
		flag = NilFlag
	case types.KindMinNotNull:
		flag = BytesFlag
	case types.KindMaxValue:
		flag = MaxFlag
	default:
		err = errors.Errorf("unsupport encode type %d", tp)
	}
	return
}

func (encoder *Encoder) encodeBytes(buf []byte) []byte {
	r := &encoder.row
	buf = append(buf, CodecVer)
	flag := byte(0)
	if r.large {
		flag = 1
	}
	buf = append(buf, flag)
	buf = append(buf, byte(r.numNotNullCols), byte(r.numNotNullCols>>8))
	buf = append(buf, byte(r.numNullCols), byte(r.numNullCols>>8))
	buf = append(buf, r.valFlags...)
	if r.large {
		buf = append(buf, u32SliceToBytes(r.colIDs32)...)
		buf = append(buf, u32SliceToBytes(r.offsets32)...)
	} else {
		buf = append(buf, r.colIDs...)
		buf = append(buf, u16SliceToBytes(r.offsets)...)
	}
	buf = append(buf, r.data...)
	return buf
}

func (encoder *Encoder) initValFlags() {
	if cap(encoder.valFlags) >= int(encoder.numNotNullCols) {
		encoder.valFlags = encoder.valFlags[:encoder.numNotNullCols]
	} else {
		encoder.valFlags = make([]byte, encoder.numNotNullCols)
	}
}

func (encoder *Encoder) initColIDs() {
	numCols := int(encoder.numNotNullCols + encoder.numNullCols)
	if cap(encoder.colIDs) >= numCols {
		encoder.colIDs = encoder.colIDs[:numCols]
	} else {
		encoder.colIDs = make([]byte, numCols)
	}
}

func (encoder *Encoder) initColIDs32() {
	numCols := int(encoder.numNotNullCols + encoder.numNullCols)
	if cap(encoder.colIDs32) >= numCols {
		encoder.colIDs32 = encoder.colIDs32[:numCols]
	} else {
		encoder.colIDs32 = make([]uint32, numCols)
	}
}

func (encoder *Encoder) initOffsets() {
	if cap(encoder.offsets) >= int(encoder.numNotNullCols) {
		encoder.offsets = encoder.offsets[:encoder.numNotNullCols]
	} else {
		encoder.offsets = make([]uint16, encoder.numNotNullCols)
	}
}

func (encoder *Encoder) initOffsets32() {
	if cap(encoder.offsets32) >= int(encoder.numNotNullCols) {
		encoder.offsets32 = encoder.offsets32[:encoder.numNotNullCols]
	} else {
		encoder.offsets32 = make([]uint32, encoder.numNotNullCols)
	}
}

var defaultStmtCtx = &stmtctx.StatementContext{
	TimeZone: time.Local,
}

// IsNewFormat checks whether row data is in new-format.
func IsNewFormat(rowData []byte) bool {
	if len(rowData) == 0 {
		return true
	}
	return rowData[0] == CodecVer
}

// RowToOldRow converts a row to old-format row.
func RowToOldRow(rowData, buf []byte) ([]byte, error) {
	if len(rowData) == 0 || !IsNewFormat(rowData) {
		return rowData, nil
	}
	buf = buf[:0]
	var r row
	err := r.setRowData(rowData)
	if err != nil {
		return nil, err
	}
	if !r.large {
		for i, colID := range r.colIDs {
			buf = append(buf, VarintFlag)
			buf = codec.EncodeVarint(buf, int64(colID))
			if i < int(r.numNotNullCols) {
				val := r.getData(i)
				switch r.valFlags[i] {
				case BytesFlag:
					buf = append(buf, CompactBytesFlag)
					buf = codec.EncodeCompactBytes(buf, val)
				case IntFlag:
					buf = append(buf, VarintFlag)
					buf = codec.EncodeVarint(buf, decodeInt(val))
				case UintFlag:
					buf = append(buf, VaruintFlag)
					buf = codec.EncodeUvarint(buf, decodeUint(val))
				default:
					buf = append(buf, r.valFlags[i])
					buf = append(buf, val...)
				}
			} else {
				buf = append(buf, NilFlag)
			}
		}
	} else {
		for i, colID := range r.colIDs32 {
			buf = append(buf, VarintFlag)
			buf = codec.EncodeVarint(buf, int64(colID))
			if i < int(r.numNotNullCols) {
				val := r.getData(i)
				switch r.valFlags[i] {
				case BytesFlag:
					buf = append(buf, CompactBytesFlag)
					buf = codec.EncodeCompactBytes(buf, val)
				case IntFlag:
					buf = append(buf, VarintFlag)
					buf = codec.EncodeVarint(buf, decodeInt(val))
				case UintFlag:
					buf = append(buf, VaruintFlag)
					buf = codec.EncodeUvarint(buf, decodeUint(val))
				default:
					buf = append(buf, r.valFlags[i])
					buf = append(buf, val...)
				}
			} else {
				buf = append(buf, NilFlag)
			}
		}
	}
	if len(buf) == 0 {
		buf = append(buf, NilFlag)
	}
	return buf, nil
}
