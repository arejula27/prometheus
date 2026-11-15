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
	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

// RW2Encoder encodes samples into a Remote Write 2.0 Protobuf message the
// same way the queue manager does: one TimeSeries per sample, labels
// interned into a per-request symbols table, OptimizedMarshal into a reused
// buffer.
//
// It is not safe for concurrent use.
type RW2Encoder struct {
	symbols writev2.SymbolsTable
	series  []writev2.TimeSeries
	buf     []byte
}

// NewRW2Encoder returns a new RW2Encoder.
func NewRW2Encoder() *RW2Encoder {
	return &RW2Encoder{symbols: writev2.NewSymbolTable()}
}

// Encode returns the uncompressed message. The returned slice is reused by
// the next call; callers must copy it if they need to retain it.
func (e *RW2Encoder) Encode(samples []Sample) ([]byte, error) {
	e.symbols.Reset()
	if cap(e.series) < len(samples) {
		e.series = make([]writev2.TimeSeries, len(samples))
		for i := range e.series {
			e.series[i].Samples = []writev2.Sample{{}}
		}
	}
	e.series = e.series[:len(samples)]
	for i, s := range samples {
		ts := &e.series[i]
		ts.LabelsRefs = e.symbols.SymbolizeLabels(s.Labels, ts.LabelsRefs)
		ts.Samples = ts.Samples[:1]
		ts.Samples[0] = writev2.Sample{Value: s.V, Timestamp: s.T}
	}
	req := writev2.Request{Symbols: e.symbols.Symbols(), Timeseries: e.series}
	var err error
	e.buf, err = req.OptimizedMarshal(e.buf)
	return e.buf, err
}

// RW2Decoder decodes Remote Write 2.0 Protobuf messages like the remote
// write handler does.
//
// It is not safe for concurrent use.
type RW2Decoder struct {
	b labels.ScratchBuilder
}

// NewRW2Decoder returns a new RW2Decoder.
func NewRW2Decoder() *RW2Decoder {
	return &RW2Decoder{b: labels.NewScratchBuilder(0)}
}

// Decode unmarshals the uncompressed message and calls visit for every
// float sample.
func (d *RW2Decoder) Decode(data []byte, visit VisitFunc) error {
	var req writev2.Request
	if err := req.Unmarshal(data); err != nil {
		return err
	}
	for _, ts := range req.Timeseries {
		ls, err := ts.ToLabels(&d.b, req.Symbols)
		if err != nil {
			return err
		}
		for _, s := range ts.Samples {
			visit(ls, s.Timestamp, s.Value)
		}
	}
	return nil
}
