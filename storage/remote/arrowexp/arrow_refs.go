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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

// The "refs" layout is a direct columnar translation of the Remote Write 2.0
// message. A request is two Arrow IPC streams prefixed by the uvarint length
// of the first one:
//
//	symbols stream: {symbol: utf8}                          one row per symbol
//	samples stream: {labels_refs: list<uint32>, timestamp: int64, value: float64}
//	                                                        one row per sample
//
// labels_refs holds name/value pairs of indexes into the symbols column,
// exactly like writev2.TimeSeries.LabelsRefs.
var (
	refsSymbolsSchema = arrow.NewSchema([]arrow.Field{
		{Name: "symbol", Type: arrow.BinaryTypes.String},
	}, nil)
	refsSamplesSchema = arrow.NewSchema([]arrow.Field{
		{Name: "labels_refs", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint32)},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
)

// ArrowRefsEncoder encodes samples using the "refs" layout.
//
// It is not safe for concurrent use.
type ArrowRefsEncoder struct {
	mem     memory.Allocator
	symbols writev2.SymbolsTable
	refs    []uint32

	symB  *array.RecordBuilder
	dataB *array.RecordBuilder

	symBuf, dataBuf bytes.Buffer
	out             []byte
}

// NewArrowRefsEncoder returns a new ArrowRefsEncoder.
func NewArrowRefsEncoder() *ArrowRefsEncoder {
	mem := memory.NewGoAllocator()
	return &ArrowRefsEncoder{
		mem:     mem,
		symbols: writev2.NewSymbolTable(),
		symB:    array.NewRecordBuilder(mem, refsSymbolsSchema),
		dataB:   array.NewRecordBuilder(mem, refsSamplesSchema),
	}
}

// Encode returns the uncompressed message. The returned slice is reused by
// the next call; callers must copy it if they need to retain it.
func (e *ArrowRefsEncoder) Encode(samples []Sample) ([]byte, error) {
	e.symbols.Reset()

	listB := e.dataB.Field(0).(*array.ListBuilder)
	refB := listB.ValueBuilder().(*array.Uint32Builder)
	tsB := e.dataB.Field(1).(*array.Int64Builder)
	valB := e.dataB.Field(2).(*array.Float64Builder)
	listB.Reserve(len(samples))
	tsB.Reserve(len(samples))
	valB.Reserve(len(samples))

	for _, s := range samples {
		e.refs = e.symbols.SymbolizeLabels(s.Labels, e.refs)
		listB.Append(true)
		refB.AppendValues(e.refs, nil)
		tsB.UnsafeAppend(s.T)
		valB.UnsafeAppend(s.V)
	}
	dataRec := e.dataB.NewRecordBatch()
	defer dataRec.Release()

	symCol := e.symB.Field(0).(*array.StringBuilder)
	symCol.AppendValues(e.symbols.Symbols(), nil)
	symRec := e.symB.NewRecordBatch()
	defer symRec.Release()

	e.symBuf.Reset()
	if err := writeIPC(&e.symBuf, e.mem, refsSymbolsSchema, symRec); err != nil {
		return nil, err
	}
	e.dataBuf.Reset()
	if err := writeIPC(&e.dataBuf, e.mem, refsSamplesSchema, dataRec); err != nil {
		return nil, err
	}

	e.out = binary.AppendUvarint(e.out[:0], uint64(e.symBuf.Len()))
	e.out = append(e.out, e.symBuf.Bytes()...)
	e.out = append(e.out, e.dataBuf.Bytes()...)
	return e.out, nil
}

// ArrowRefsDecoder decodes messages in the "refs" layout.
//
// It is not safe for concurrent use.
type ArrowRefsDecoder struct {
	mem memory.Allocator
	b   labels.ScratchBuilder
}

// NewArrowRefsDecoder returns a new ArrowRefsDecoder.
func NewArrowRefsDecoder() *ArrowRefsDecoder {
	return &ArrowRefsDecoder{mem: memory.NewGoAllocator(), b: labels.NewScratchBuilder(0)}
}

// Decode calls visit for every sample in the uncompressed message.
func (d *ArrowRefsDecoder) Decode(data []byte, visit VisitFunc) error {
	symLen, n := binary.Uvarint(data)
	if n <= 0 || uint64(len(data)-n) < symLen {
		return errors.New("arrowexp: invalid symbols stream length")
	}
	symData := data[n : n+int(symLen)]
	sampleData := data[n+int(symLen):]

	symR, err := ipc.NewReader(bytes.NewReader(symData), ipc.WithAllocator(d.mem), ipc.WithSchema(refsSymbolsSchema))
	if err != nil {
		return err
	}
	defer symR.Release()
	if !symR.Next() {
		return fmt.Errorf("arrowexp: missing symbols record: %w", symR.Err())
	}
	symbols := symR.RecordBatch().Column(0).(*array.String)

	dataR, err := ipc.NewReader(bytes.NewReader(sampleData), ipc.WithAllocator(d.mem), ipc.WithSchema(refsSamplesSchema))
	if err != nil {
		return err
	}
	defer dataR.Release()
	for dataR.Next() {
		rec := dataR.RecordBatch()
		list := rec.Column(0).(*array.List)
		refs := list.ListValues().(*array.Uint32).Uint32Values()
		offsets := list.Offsets()
		ts := rec.Column(1).(*array.Int64).Int64Values()
		vals := rec.Column(2).(*array.Float64).Float64Values()
		for i := range int(rec.NumRows()) {
			row := refs[offsets[i]:offsets[i+1]]
			if len(row)%2 != 0 {
				return fmt.Errorf("arrowexp: odd number of label refs in row %d", i)
			}
			d.b.Reset()
			for j := 0; j < len(row); j += 2 {
				nameRef, valueRef := int(row[j]), int(row[j+1])
				if nameRef >= symbols.Len() || valueRef >= symbols.Len() {
					return fmt.Errorf("arrowexp: label ref out of range in row %d", i)
				}
				d.b.Add(symbols.Value(nameRef), symbols.Value(valueRef))
			}
			visit(d.b.Labels(), ts[i], vals[i])
		}
	}
	return dataR.Err()
}

func writeIPC(buf *bytes.Buffer, mem memory.Allocator, schema *arrow.Schema, rec arrow.RecordBatch) error {
	w := ipc.NewWriter(buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		return err
	}
	return w.Close()
}
