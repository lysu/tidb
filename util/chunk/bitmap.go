// Copyright 2018 PingCAP, Inc.
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
package chunk

type Bitmap struct {
	bits []byte
	pos byte
}

type MapChecker struct {
	idx    int
	bits   byte
	pos    byte
	mapRef []byte
}

func NewBitMap(cap int) *Bitmap {
	return &Bitmap{
		bits:make([]byte, 0, cap/8),
	}
}

func (b *Bitmap) MapChecker() *MapChecker {
	return &MapChecker{
		mapRef: b.bits,
		pos: 1,
	}
}

func (m *MapChecker) NextNoMarked() bool {
	if m.pos == 1 {
		m.bits = m.mapRef[m.idx]
		m.idx++
	}
	marked := m.bits & m.pos == 0
	if m.pos == 128 {
		m.pos = 1
	} else {
		m.pos <<= 1
	}
	return marked
}

func (b *Bitmap) Append(marked bool) {
	if b.pos == 0 {
		b.bits = append(b.bits, 0)
	}
	if marked {
		b.bits[len(b.bits)-1] |= byte(1 << b.pos)
	}
	if b.pos == 7 {
		b.pos = 0
		return
	}
	b.pos++
}

func (b *Bitmap) Reset()  {
	b.bits = b.bits[:0]
	b.pos = 0
}
