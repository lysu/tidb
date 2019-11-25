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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

// Decoder decodes the row to chunk.Chunk.
type Decoder struct {
	row
	columns     []ColInfo
	handleColID int64
	loc         *time.Location
}

// NewDecoder creates a NewDecoder.
// requestColIDs is the columnIDs to decode. tps is the field types for request columns.
// origDefault is the original default value in old format, if the column ID is not found in the row,
// the origDefault will be used.
func NewDecoder(columns []ColInfo, handleColID int64, loc *time.Location) *Decoder {
	return &Decoder{
		columns:     columns,
		handleColID: handleColID,
		loc:         loc,
	}
}

// ColInfo is row codec param to provide column info.
type ColInfo struct {
	ID           int64
	Tp           int32
	Flag         int32
	IsPKHandle   bool
	DefaultValue func() ([]byte, error)

	Flen    int
	Decimal int
	Elems   []string
}

var defaultStmtCtx = &stmtctx.StatementContext{
	TimeZone: time.Local,
}

// DecodeToDatumMap decodes byte slices to datum map.
func (decoder *Decoder) DecodeToDatumMap(rowData []byte, handle int64, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(decoder.columns))
	}
	err := decoder.setRowData(rowData)
	if err != nil {
		return nil, err
	}
	for _, col := range decoder.columns {
		if col.ID == decoder.handleColID {
			row[col.ID] = types.NewIntDatum(handle)
			continue
		}
		idx, isNil, notFound := decoder.row.findColID(col.ID)
		if !notFound && !isNil {
			colData := decoder.getData(idx)
			d, err := decoder.decodeColDatum(&col, colData)
			if err != nil {
				return nil, err
			}
			row[col.ID] = *d
			continue
		}

		if isNil {
			var d types.Datum
			d.SetNull()
			row[col.ID] = d
			continue
		}

		if col.DefaultValue == nil {
			continue
		}
		defaultVal, err := col.DefaultValue()
		if err != nil {
			return nil, err
		}

		if len(defaultVal) == 0 {
			var d types.Datum
			d.SetNull()
			row[col.ID] = d
			continue
		}

		d, err := decoder.decodeColDatum(&col, defaultVal)
		if err != nil {
			return nil, err
		}
		row[col.ID] = *d
	}
	return row, nil
}

// DecodeToChunk decodes a row to chunk.
func (decoder *Decoder) DecodeToChunk(rowData []byte, handle int64, chk *chunk.Chunk) error {
	err := decoder.setRowData(rowData)
	if err != nil {
		return err
	}

	for colIdx, col := range decoder.columns {
		if col.ID == decoder.handleColID {
			chk.AppendInt64(colIdx, handle)
			continue
		}

		idx, isNil, notFound := decoder.row.findColID(col.ID)
		if !notFound && !isNil {
			colData := decoder.getData(idx)
			err := decoder.decodeColData(colIdx, &col, colData, chk)
			if err != nil {
				return err
			}
			continue
		}

		if isNil {
			chk.AppendNull(colIdx)
			continue
		}

		if col.DefaultValue == nil {
			continue
		}
		defaultVal, err := col.DefaultValue()
		if err != nil {
			return err
		}

		if len(defaultVal) == 0 {
			chk.AppendNull(colIdx)
			continue
		}

		err = decoder.decodeColData(colIdx, &col, defaultVal, chk)
		if err != nil {
			return err
		}
	}
	return nil
}

func (decoder *Decoder) decodeColData(colIdx int, col *ColInfo, colData []byte, chk *chunk.Chunk) error {
	switch byte(col.Tp) {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny, mysql.TypeYear:
		if mysql.HasUnsignedFlag(uint(col.Flag)) {
			chk.AppendUint64(colIdx, decodeUint(colData))
		} else {
			chk.AppendInt64(colIdx, decodeInt(colData))
		}
	case mysql.TypeFloat:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return err
		}
		chk.AppendFloat32(colIdx, float32(fVal))
	case mysql.TypeDouble:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return err
		}
		chk.AppendFloat64(colIdx, fVal)
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		chk.AppendBytes(colIdx, colData)
	case mysql.TypeNewDecimal:
		_, dec, _, _, err := codec.DecodeDecimal(colData)
		if err != nil {
			return err
		}
		chk.AppendMyDecimal(colIdx, dec)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t types.Time
		t.Type = uint8(col.Tp)
		t.Fsp = int8(col.Decimal)
		err := t.FromPackedUint(decodeUint(colData))
		if err != nil {
			return err
		}
		if byte(col.Tp) == mysql.TypeTimestamp && decoder.loc != nil && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, decoder.loc)
			if err != nil {
				return err
			}
		}
		chk.AppendTime(colIdx, t)
	case mysql.TypeDuration:
		var dur types.Duration
		dur.Duration = time.Duration(decodeInt(colData))
		dur.Fsp = int8(col.Decimal)
		chk.AppendDuration(colIdx, dur)
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(col.Elems, decodeUint(colData))
		if err != nil {
			enum = types.Enum{}
		}
		chk.AppendEnum(colIdx, enum)
	case mysql.TypeSet:
		set, err := types.ParseSetValue(col.Elems, decodeUint(colData))
		if err != nil {
			return err
		}
		chk.AppendSet(colIdx, set)
	case mysql.TypeBit:
		byteSize := (col.Flen + 7) >> 3
		chk.AppendBytes(colIdx, types.NewBinaryLiteralFromUint(decodeUint(colData), byteSize))
	case mysql.TypeJSON:
		var j json.BinaryJSON
		j.TypeCode = colData[0]
		j.Value = colData[1:]
		chk.AppendJSON(colIdx, j)
	default:
		return errors.Errorf("unknown type %d", col.Tp)
	}
	return nil
}

func (decoder *Decoder) decodeColDatum(col *ColInfo, colData []byte) (*types.Datum, error) {
	var d types.Datum
	switch byte(col.Tp) {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny, mysql.TypeYear:
		if mysql.HasUnsignedFlag(uint(col.Flag)) {
			d.SetUint64(decodeUint(colData))
		} else {
			d.SetInt64(decodeInt(colData))
		}
	case mysql.TypeFloat:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return nil, err
		}
		d.SetFloat32(float32(fVal))
	case mysql.TypeDouble:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return nil, err
		}
		d.SetFloat64(fVal)
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetBytes(colData)
	case mysql.TypeNewDecimal:
		_, dec, _, _, err := codec.DecodeDecimal(colData)
		if err != nil {
			return nil, err
		}
		d.SetMysqlDecimal(dec)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t types.Time
		t.Type = uint8(col.Tp)
		t.Fsp = int8(col.Decimal)
		err := t.FromPackedUint(decodeUint(colData))
		if err != nil {
			return nil, err
		}
		if byte(col.Tp) == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, decoder.loc)
			if err != nil {
				return nil, err
			}
		}
		d.SetMysqlTime(t)
	case mysql.TypeDuration:
		var dur types.Duration
		dur.Duration = time.Duration(decodeInt(colData))
		dur.Fsp = int8(col.Decimal)
		d.SetMysqlDuration(dur)
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(col.Elems, decodeUint(colData))
		if err != nil {
			enum = types.Enum{}
		}
		d.SetMysqlEnum(enum)
	case mysql.TypeSet:
		set, err := types.ParseSetValue(col.Elems, decodeUint(colData))
		if err != nil {
			return nil, err
		}
		d.SetMysqlSet(set)
	case mysql.TypeBit:
		byteSize := (col.Flen + 7) >> 3
		d.SetBinaryLiteral(types.NewBinaryLiteralFromUint(decodeUint(colData), byteSize))
	case mysql.TypeJSON:
		var j json.BinaryJSON
		j.TypeCode = colData[0]
		j.Value = colData[1:]
		d.SetMysqlJSON(j)
	default:
		return nil, errors.Errorf("unknown type %d", col.Tp)
	}
	return &d, nil
}

// DecodeToBytes decodes raw byte slice to row data.
func (decoder *Decoder) DecodeToBytes(outputOffset map[int64]int, handle int64, value []byte, cacheBytes []byte) ([][]byte, error) {
	var r row
	err := r.setRowData(value)
	if err != nil {
		return nil, err
	}
	values := make([][]byte, len(outputOffset))
	for _, col := range decoder.columns {
		var err error
		tp, err := FieldType2Flag(byte(col.Tp), uint(col.Flag))
		if err != nil {
			return nil, err
		}
		colID := col.ID
		offset := outputOffset[colID]
		if col.IsPKHandle || colID == model.ExtraHandleID {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(uint(col.Flag)) {
				// PK column is Unsigned.
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleData, err1 := codec.EncodeValue(nil, cacheBytes, handleDatum)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			values[offset] = handleData
			continue
		}

		idx, isNil, notFound := r.findColID(colID)
		if !notFound && !isNil {
			val := r.getData(idx)
			var buf []byte
			switch tp {
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
				buf = append(buf, tp)
				buf = append(buf, val...)
			}
			values[offset] = buf
			continue
		}

		if isNil {
			values[offset] = []byte{NilFlag}
			continue
		}

		if col.DefaultValue != nil {
			defVal, err := col.DefaultValue()
			if err != nil {
				return nil, err
			}
			if len(defVal) > 0 {
				values[offset] = defVal
				continue
			}
		}
		if mysql.HasNotNullFlag(uint(col.Flag)) {
			return nil, errors.Errorf("Miss column %d", colID)
		}
		values[offset] = []byte{NilFlag}
	}
	return values, nil
}
