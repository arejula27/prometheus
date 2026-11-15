// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrowexp

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prometheus/prometheus/model/labels"
)

// The "wide" layout is the Arrow-native one: every label name present in the
// request becomes its own nullable dictionary<int32, utf8> column, sorted by
// name, followed by timestamp and value columns. A series without a given
// label has a null in that column. The schema therefore changes from request
// to request and is sent with each one (stateless).
//
//	{<label name>: dictionary<int32, utf8>, ..., timestamp: int64, value: float64}
//
// Label columns are prefixed with labelColumnPrefix so they cannot clash with
// the timestamp and value columns.
const labelColumnPrefix = "l:"

var wideDictType = &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}

// wideColumn accumulates one label column. Instead of using Arrow's
// dictionary builder, which converts every appended string to []byte and
// hashes it with a generic memo table, the dictionary is built with a plain
// Go map and the Arrow buffers are assembled directly from reused slices.
type wideColumn struct {
	name     string
	used     bool // Whether the column appears in the current request.
	valueIdx map[string]int32
	values   []string
	indices  []int32
	validity []byte

	// Last value set, to skip the map lookup for runs of equal values, which
	// are common because consecutive samples often come from the same target.
	lastValue string
	lastIdx   int32
	hasLast   bool

	// Dictionary values buffers, rebuilt from values for every request.
	offsets []int32
	data    []byte
}

// reset prepares the column for a request of n rows, keeping capacity.
func (c *wideColumn) reset(n int) {
	c.used = true
	clear(c.valueIdx)
	c.values = c.values[:0]
	c.hasLast = false
	c.indices = slices.Grow(c.indices[:0], n)[:n]
	clear(c.indices)
	nb := int(bitutil.BytesForBits(int64(n)))
	c.validity = slices.Grow(c.validity[:0], nb)[:nb]
	clear(c.validity)
}

func (c *wideColumn) set(row int, value string) {
	if !c.hasLast || c.lastValue != value {
		idx, ok := c.valueIdx[value]
		if !ok {
			idx = int32(len(c.values))
			c.values = append(c.values, value)
			c.valueIdx[value] = idx
		}
		c.lastValue, c.lastIdx, c.hasLast = value, idx, true
	}
	c.indices[row] = c.lastIdx
	bitutil.SetBit(c.validity, row)
}

// arrayData assembles the Arrow array data for this column. The returned data
// references the column's slices, so it must not outlive the next reset.
func (c *wideColumn) arrayData(n int) arrow.ArrayData {
	c.offsets = append(c.offsets[:0], 0)
	c.data = c.data[:0]
	for _, v := range c.values {
		c.data = append(c.data, v...)
		c.offsets = append(c.offsets, int32(len(c.data)))
	}
	dict := array.NewData(arrow.BinaryTypes.String, len(c.values), []*memory.Buffer{
		nil,
		memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(c.offsets)),
		memory.NewBufferBytes(c.data),
	}, nil, 0, 0)
	defer dict.Release()

	nulls := n - bitutil.CountSetBits(c.validity, 0, n)
	var validity *memory.Buffer
	if nulls > 0 {
		validity = memory.NewBufferBytes(c.validity)
	}
	return array.NewDataWithDictionary(wideDictType, n, []*memory.Buffer{
		validity,
		memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(c.indices)),
	}, nulls, 0, dict)
}

// ArrowWideEncoder encodes samples using the "wide" layout.
//
// It is not safe for concurrent use.
type ArrowWideEncoder struct {
	mem     memory.Allocator
	columns map[string]*wideColumn
	active  []*wideColumn

	// State used by appendLabel while ranging over one sample's labels.
	row int
	n   int
	pos int
	// byPos caches the column used at each label position by the previous
	// sample, to skip the column map lookup when label names repeat.
	byPos []*wideColumn

	ts   []int64
	vals []float64
	buf  bytes.Buffer
}

// NewArrowWideEncoder returns a new ArrowWideEncoder.
func NewArrowWideEncoder() *ArrowWideEncoder {
	return &ArrowWideEncoder{
		mem:     memory.NewGoAllocator(),
		columns: map[string]*wideColumn{},
	}
}

func (e *ArrowWideEncoder) appendLabel(l labels.Label) {
	var c *wideColumn
	if e.pos < len(e.byPos) && e.byPos[e.pos].name == l.Name {
		c = e.byPos[e.pos]
	} else {
		var ok bool
		c, ok = e.columns[l.Name]
		if !ok {
			c = &wideColumn{name: l.Name, valueIdx: map[string]int32{}}
			e.columns[l.Name] = c
		}
		if e.pos < len(e.byPos) {
			e.byPos[e.pos] = c
		} else {
			e.byPos = append(e.byPos, c)
		}
	}
	e.pos++
	if !c.used {
		c.reset(e.n)
		e.active = append(e.active, c)
	}
	c.set(e.row, l.Value)
}

// Encode returns the uncompressed message. The returned slice is reused by
// the next call; callers must copy it if they need to retain it.
func (e *ArrowWideEncoder) Encode(samples []Sample) ([]byte, error) {
	n := len(samples)
	e.n = n
	for _, c := range e.active {
		c.used = false
	}
	e.active = e.active[:0]
	e.ts = slices.Grow(e.ts[:0], n)[:n]
	e.vals = slices.Grow(e.vals[:0], n)[:n]

	appendLabel := e.appendLabel
	for i, s := range samples {
		e.row, e.pos = i, 0
		s.Labels.Range(appendLabel)
		e.ts[i] = s.T
		e.vals[i] = s.V
	}
	slices.SortFunc(e.active, func(a, b *wideColumn) int { return strings.Compare(a.name, b.name) })

	fields := make([]arrow.Field, 0, len(e.active)+2)
	arrs := make([]arrow.Array, 0, len(e.active)+2)
	for _, c := range e.active {
		fields = append(fields, arrow.Field{Name: labelColumnPrefix + c.name, Type: wideDictType, Nullable: true})
		d := c.arrayData(n)
		arrs = append(arrs, array.MakeFromData(d))
		d.Release()
	}
	fields = append(fields,
		arrow.Field{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	)
	tsData := array.NewData(arrow.PrimitiveTypes.Int64, n, []*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(e.ts))}, nil, 0, 0)
	valData := array.NewData(arrow.PrimitiveTypes.Float64, n, []*memory.Buffer{nil, memory.NewBufferBytes(arrow.Float64Traits.CastToBytes(e.vals))}, nil, 0, 0)
	arrs = append(arrs, array.MakeFromData(tsData), array.MakeFromData(valData))
	tsData.Release()
	valData.Release()

	schema := arrow.NewSchema(fields, nil)
	rec := array.NewRecordBatch(schema, arrs, int64(n))
	for _, a := range arrs {
		a.Release()
	}
	defer rec.Release()

	e.buf.Reset()
	if err := writeIPC(&e.buf, e.mem, schema, rec); err != nil {
		return nil, err
	}
	return e.buf.Bytes(), nil
}

// ArrowWideDecoder decodes messages in the "wide" layout.
//
// It is not safe for concurrent use.
type ArrowWideDecoder struct {
	mem  memory.Allocator
	b    labels.ScratchBuilder
	cols []wideLabelColumn
}

// NewArrowWideDecoder returns a new ArrowWideDecoder.
func NewArrowWideDecoder() *ArrowWideDecoder {
	return &ArrowWideDecoder{mem: memory.NewGoAllocator(), b: labels.NewScratchBuilder(0)}
}

type wideLabelColumn struct {
	name     string
	indices  []int32
	validity []byte // Nil when the column has no nulls.
	offset   int
	dict     *array.String
}

// Decode calls visit for every sample in the uncompressed message.
func (d *ArrowWideDecoder) Decode(data []byte, visit VisitFunc) error {
	r, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(d.mem))
	if err != nil {
		return err
	}
	defer r.Release()

	for r.Next() {
		rec := r.RecordBatch()
		schema := rec.Schema()
		d.cols = d.cols[:0]
		var ts []int64
		var vals []float64
		for i, f := range schema.Fields() {
			switch {
			case f.Name == "timestamp":
				ts = rec.Column(i).(*array.Int64).Int64Values()
			case f.Name == "value":
				vals = rec.Column(i).(*array.Float64).Float64Values()
			case strings.HasPrefix(f.Name, labelColumnPrefix):
				dict, ok := rec.Column(i).(*array.Dictionary)
				if !ok {
					return fmt.Errorf("arrowexp: label column %q is not dictionary encoded", f.Name)
				}
				idx, ok := dict.Indices().(*array.Int32)
				if !ok {
					return fmt.Errorf("arrowexp: label column %q must have int32 indices", f.Name)
				}
				var validity []byte
				if dict.NullN() > 0 {
					validity = dict.NullBitmapBytes()
				}
				d.cols = append(d.cols, wideLabelColumn{
					name:     f.Name[len(labelColumnPrefix):],
					indices:  idx.Int32Values(),
					validity: validity,
					offset:   dict.Data().Offset(),
					dict:     dict.Dictionary().(*array.String),
				})
			default:
				return fmt.Errorf("arrowexp: unexpected column %q", f.Name)
			}
		}
		if ts == nil || vals == nil {
			return fmt.Errorf("arrowexp: missing timestamp or value column")
		}
		for row := range int(rec.NumRows()) {
			d.b.Reset()
			for i := range d.cols {
				c := &d.cols[i]
				if c.validity != nil && !bitutil.BitIsSet(c.validity, c.offset+row) {
					continue
				}
				d.b.Add(c.name, c.dict.Value(int(c.indices[row])))
			}
			// Columns are sorted by name already, so no need to sort labels.
			visit(d.b.Labels(), ts[row], vals[row])
		}
	}
	return r.Err()
}
