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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// FieldType2Flag transforms field type into type flag.
func FieldType2Flag(tp byte, f uint) (flag byte, err error) {
	var kind byte
	kind, err = type2Kind(tp, f)
	if err != nil {
		return
	}
	switch kind {
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
		err = errors.Errorf("unsupport encode type %d", kind)
	}
	return
}

func type2Kind(tp byte, flag uint) (kind byte, err error) {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if flag&mysql.UniqueFlag == 0 {
			kind = types.KindInt64
		} else {
			kind = types.KindUint64
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		kind = types.KindFloat64
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		kind = types.KindString
	case mysql.TypeDatetime, mysql.TypeDate, mysql.TypeTimestamp:
		kind = types.KindMysqlTime
	case mysql.TypeDuration:
		kind = types.KindMysqlDuration
	case mysql.TypeNewDecimal:
		kind = types.KindMysqlDecimal
	case mysql.TypeYear:
		kind = types.KindInt64
	case mysql.TypeEnum:
		kind = types.KindMysqlEnum
	case mysql.TypeBit:
		kind = types.KindMysqlBit
	case mysql.TypeSet:
		kind = types.KindMysqlSet
	case mysql.TypeJSON:
		kind = types.KindMysqlJSON
	case mysql.TypeNull:
		kind = types.KindNull
	default:
		err = errors.Errorf("unknown field type %d", tp)
	}
	return
}
