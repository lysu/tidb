package chunk

import (
	"testing"
	"fmt"
)

func TestNewBitMap(t *testing.T) {
	bm := NewBitMap(1024)
	for i := 0; i < 10; i++ {
		bm.Append(true)
	}
	for i := 0; i < 10; i++ {
		bm.Append(false)
	}
	c := bm.MapChecker()
	for i := 0; i < 20; i++ {
		fmt.Println(c.NextNoMarked())
	}
}
