package chunk_test

import (
	"fmt"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	c := chunk.NewTestC()
	d := types.NewD(2, "", 0, 0, -9223372036854775808, nil, nil)
	s := time.Now()
	x := c.UpperBound(0, &d)
	fmt.Println(x, time.Since(s).Nanoseconds())

	h := &statistics.Histogram{
		Bounds: c,
		Tp: &types.FieldType{
			Tp: 8, Flag: 547, Flen: 11, Decimal: 0, Charset: "binary", Collate: "binary", Elems: nil,
		},
	}
	ran := []*ranger.Range{
		{
			LowVal:      []types.Datum{types.NewD(2, "", 0, 0, 0, nil, nil)},
			HighVal:     []types.Datum{types.NewD(2, "", 0, 0, -1, nil, nil)},
			LowExclude:  false,
			HighExclude: false,
		},
	}
	h.SplitRange(nil, ran, false)
	fmt.Println("ok?")
}
