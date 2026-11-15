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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/compression"
)

type encoder interface {
	Encode([]Sample) ([]byte, error)
}

type decoder interface {
	Decode([]byte, VisitFunc) error
}

var codecs = []struct {
	name   string
	newEnc func() encoder
	newDec func() decoder
}{
	{"rw2proto", func() encoder { return NewRW2Encoder() }, func() decoder { return NewRW2Decoder() }},
	{"arrowrefs", func() encoder { return NewArrowRefsEncoder() }, func() decoder { return NewArrowRefsDecoder() }},
	{"arrowwide", func() encoder { return NewArrowWideEncoder() }, func() decoder { return NewArrowWideDecoder() }},
}

var datasets = []struct {
	name string
	gen  func(int) []Sample
}{
	{"lowcard", GenerateLowCardinality},
	{"highcard", GenerateHighCardinality},
}

// batchSizes covers a small batch, the default max_samples_per_send (2000)
// and a large batch.
var batchSizes = []int{500, 2000, 10000}

func TestRoundTrip(t *testing.T) {
	for _, ds := range datasets {
		in := ds.gen(2000)
		for _, c := range codecs {
			t.Run(ds.name+"/"+c.name, func(t *testing.T) {
				enc, dec := c.newEnc(), c.newDec()
				// Encode twice to check that encoder state is reset between requests.
				for range 2 {
					data, err := enc.Encode(in)
					require.NoError(t, err)

					var got []Sample
					require.NoError(t, dec.Decode(data, func(ls labels.Labels, ts int64, v float64) {
						got = append(got, Sample{Labels: ls.Copy(), T: ts, V: v})
					}))
					require.Len(t, got, len(in))
					for i := range in {
						require.Equal(t, in[i].Labels.String(), got[i].Labels.String(), "sample %d", i)
						require.Equal(t, in[i].T, got[i].T, "sample %d", i)
						require.Equal(t, in[i].V, got[i].V, "sample %d", i)
					}
				}
			})
		}
	}
}

// Recommended CLI invocation:
//
//	go test -run '^$' -bench . -benchmem -count 6 | tee bench.txt
//	benchstat -col /codec bench.txt
func BenchmarkEncode(b *testing.B) {
	for _, ds := range datasets {
		for _, n := range batchSizes {
			in := ds.gen(n)
			for _, compr := range []compression.Type{compression.Snappy, compression.Zstd} {
				for _, c := range codecs {
					b.Run(fmt.Sprintf("data=%s/samples=%d/compr=%s/codec=%s", ds.name, n, compr, c.name), func(b *testing.B) {
						enc := c.newEnc()
						cBuf := compression.NewSyncEncodeBuffer()
						var raw, compressed int
						for b.Loop() {
							data, err := enc.Encode(in)
							if err != nil {
								b.Fatal(err)
							}
							out, err := compression.Encode(compr, data, cBuf)
							if err != nil {
								b.Fatal(err)
							}
							raw, compressed = len(data), len(out)
						}
						b.ReportMetric(float64(raw), "rawB/op")
						b.ReportMetric(float64(compressed), "wireB/op")
					})
				}
			}
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, ds := range datasets {
		for _, n := range batchSizes {
			in := ds.gen(n)
			for _, compr := range []compression.Type{compression.Snappy, compression.Zstd} {
				for _, c := range codecs {
					b.Run(fmt.Sprintf("data=%s/samples=%d/compr=%s/codec=%s", ds.name, n, compr, c.name), func(b *testing.B) {
						data, err := c.newEnc().Encode(in)
						if err != nil {
							b.Fatal(err)
						}
						wire, err := compression.Encode(compr, data, compression.NewSyncEncodeBuffer())
						if err != nil {
							b.Fatal(err)
						}
						wire = append([]byte(nil), wire...)

						dec := c.newDec()
						dBuf := compression.NewSyncDecodeBuffer()
						var sink float64
						visit := func(_ labels.Labels, _ int64, v float64) { sink += v }
						for b.Loop() {
							raw, err := compression.Decode(compr, wire, dBuf)
							if err != nil {
								b.Fatal(err)
							}
							if err := dec.Decode(raw, visit); err != nil {
								b.Fatal(err)
							}
						}
						_ = sink
					})
				}
			}
		}
	}
}
