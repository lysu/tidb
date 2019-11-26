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
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
)

// CodecVer is the constant number that represent the new row format.
const CodecVer = 128

var errInvalidCodecVer = errors.New("invalid codec version")

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag          byte = 0
	BytesFlag        byte = 1
	CompactBytesFlag byte = 2
	IntFlag          byte = 3
	UintFlag         byte = 4
	FloatFlag        byte = 5
	DecimalFlag      byte = 6
	VarintFlag       byte = 8
	VaruintFlag      byte = 9
	JSONFlag         byte = 10
	MaxFlag          byte = 250
)

// row is the struct type used to access the a row.
type row struct {
	// small:  colID []byte, offsets []uint16, optimized for most cases.
	// large:  colID []uint32, offsets []uint32.
	large          bool
	numNotNullCols uint16
	numNullCols    uint16
	colIDs         []byte

	offsets []uint16
	data    []byte

	// for large row
	colIDs32  []uint32
	offsets32 []uint32
}

// String implements the strings.Stringer interface.
func (r row) String() string {
	var colValStrs []string
	for i := 0; i < int(r.numNotNullCols); i++ {
		var colID, offStart, offEnd int64
		if r.large {
			colID = int64(r.colIDs32[i])
			if i != 0 {
				offStart = int64(r.offsets32[i-1])
			}
			offEnd = int64(r.offsets32[i])
		} else {
			colID = int64(r.colIDs[i])
			if i != 0 {
				offStart = int64(r.offsets[i-1])
			}
			offEnd = int64(r.offsets[i])
		}
		colValData := r.data[offStart:offEnd]
		colValStr := fmt.Sprintf("(%d:%d)", colID, colValData) // TODO: optimize for string column
		colValStrs = append(colValStrs, colValStr)
	}
	return strings.Join(colValStrs, ",")
}

func (r *row) getData(i int) []byte {
	var start, end uint32
	if r.large {
		if i > 0 {
			start = r.offsets32[i-1]
		}
		end = r.offsets32[i]
	} else {
		if i > 0 {
			start = uint32(r.offsets[i-1])
		}
		end = uint32(r.offsets[i])
	}
	return r.data[start:end]
}

func (r *row) setRowData(rowData []byte) error {
	if rowData[0] != CodecVer {
		return errInvalidCodecVer
	}
	r.large = rowData[1]&1 > 0
	r.numNotNullCols = binary.LittleEndian.Uint16(rowData[2:])
	r.numNullCols = binary.LittleEndian.Uint16(rowData[4:])
	cursor := 6
	if r.large {
		colIDsLen := int(r.numNotNullCols+r.numNullCols) * 4
		r.colIDs32 = bytesToU32Slice(rowData[cursor : cursor+colIDsLen])
		cursor += colIDsLen
		offsetsLen := int(r.numNotNullCols) * 4
		r.offsets32 = bytesToU32Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
	} else {
		colIDsLen := int(r.numNotNullCols + r.numNullCols)
		r.colIDs = rowData[cursor : cursor+colIDsLen]
		cursor += colIDsLen
		offsetsLen := int(r.numNotNullCols) * 2
		r.offsets = bytes2U16Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
	}
	r.data = rowData[cursor:]
	return nil
}

func (r *row) findColID(colID int64) (idx int, isNil, notFound bool) {
	// Search the column in not-null columns array.
	i, j := 0, int(r.numNotNullCols)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if r.large {
			v = int64(r.colIDs32[h])
		} else {
			v = int64(r.colIDs[h])
		}
		if v < colID {
			i = h + 1
		} else if v > colID {
			j = h
		} else {
			idx = h
			return
		}
	}

	// Search the column in null columns array.
	i, j = int(r.numNotNullCols), int(r.numNotNullCols+r.numNullCols)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if r.large {
			v = int64(r.colIDs32[h])
		} else {
			v = int64(r.colIDs[h])
		}
		if v < colID {
			i = h + 1
		} else if v > colID {
			j = h
		} else {
			isNil = true
			return
		}
	}
	notFound = true
	return
}

func bytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

func bytes2U16Slice(b []byte) []uint16 {
	if len(b) == 0 {
		return nil
	}
	var u16s []uint16
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u16s))
	hdr.Len = len(b) / 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u16s
}

func u16SliceToBytes(u16s []uint16) []byte {
	if len(u16s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u16s) * 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u16s[0]))
	return b
}

func u32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func encodeInt(buf []byte, iVal int64) []byte {
	var tmp [8]byte
	if int64(int8(iVal)) == iVal {
		buf = append(buf, byte(iVal))
	} else if int64(int16(iVal)) == iVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(iVal))
		buf = append(buf, tmp[:2]...)
	} else if int64(int32(iVal)) == iVal {
		binary.LittleEndian.PutUint32(tmp[:], uint32(iVal))
		buf = append(buf, tmp[:4]...)
	} else {
		binary.LittleEndian.PutUint64(tmp[:], uint64(iVal))
		buf = append(buf, tmp[:8]...)
	}
	return buf
}

func decodeInt(val []byte) int64 {
	switch len(val) {
	case 1:
		return int64(int8(val[0]))
	case 2:
		return int64(int16(binary.LittleEndian.Uint16(val)))
	case 4:
		return int64(int32(binary.LittleEndian.Uint32(val)))
	default:
		return int64(binary.LittleEndian.Uint64(val))
	}
}

func encodeUint(buf []byte, uVal uint64) []byte {
	var tmp [8]byte
	if uint64(uint8(uVal)) == uVal {
		buf = append(buf, byte(uVal))
	} else if uint64(uint16(uVal)) == uVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(uVal))
		buf = append(buf, tmp[:2]...)
	} else if uint64(uint32(uVal)) == uVal {
		binary.LittleEndian.PutUint32(tmp[:], uint32(uVal))
		buf = append(buf, tmp[:4]...)
	} else {
		binary.LittleEndian.PutUint64(tmp[:], uint64(uVal))
		buf = append(buf, tmp[:8]...)
	}
	return buf
}

func decodeUint(val []byte) uint64 {
	switch len(val) {
	case 1:
		return uint64(val[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(val))
	case 4:
		return uint64(binary.LittleEndian.Uint32(val))
	default:
		return binary.LittleEndian.Uint64(val)
	}
}

type largeNotNullSorter Encoder

func (s *largeNotNullSorter) Less(i, j int) bool {
	return s.colIDs32[i] < s.colIDs32[j]
}

func (s *largeNotNullSorter) Len() int {
	return int(s.numNotNullCols)
}

func (s *largeNotNullSorter) Swap(i, j int) {
	s.colIDs32[i], s.colIDs32[j] = s.colIDs32[j], s.colIDs32[i]
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

type smallNotNullSorter Encoder

func (s *smallNotNullSorter) Less(i, j int) bool {
	return s.colIDs[i] < s.colIDs[j]
}

func (s *smallNotNullSorter) Len() int {
	return int(s.numNotNullCols)
}

func (s *smallNotNullSorter) Swap(i, j int) {
	s.colIDs[i], s.colIDs[j] = s.colIDs[j], s.colIDs[i]
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

type smallNullSorter Encoder

func (s *smallNullSorter) Less(i, j int) bool {
	nullCols := s.colIDs[s.numNotNullCols:]
	return nullCols[i] < nullCols[j]
}

func (s *smallNullSorter) Len() int {
	return int(s.numNullCols)
}

func (s *smallNullSorter) Swap(i, j int) {
	nullCols := s.colIDs[s.numNotNullCols:]
	nullCols[i], nullCols[j] = nullCols[j], nullCols[i]
}

type largeNullSorter Encoder

func (s *largeNullSorter) Less(i, j int) bool {
	nullCols := s.colIDs32[s.numNotNullCols:]
	return nullCols[i] < nullCols[j]
}

func (s *largeNullSorter) Len() int {
	return int(s.numNullCols)
}

func (s *largeNullSorter) Swap(i, j int) {
	nullCols := s.colIDs32[s.numNotNullCols:]
	nullCols[i], nullCols[j] = nullCols[j], nullCols[i]
}

const (
	// Length of rowkey.
	rowKeyLen = 19
	// Index of record flag 'r' in rowkey used by master tidb-server.
	// The rowkey format is t{8 bytes id}_r{8 bytes handle}
	recordPrefixIdx = 10
)

// IsRowKey determine whether key is row key.
// this method will be used in unistore.
func IsRowKey(key []byte) bool {
	return len(key) == rowKeyLen && key[0] == 't' && key[recordPrefixIdx] == 'r'
}
