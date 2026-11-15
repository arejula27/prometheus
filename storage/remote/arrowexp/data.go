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

// Package arrowexp is an experiment comparing the Remote Write 2.0 Protobuf
// message with Apache Arrow based encodings for float samples.
// See https://github.com/prometheus/prometheus/issues/14342.
//
// The package is a separate Go module so that the Arrow dependency does not
// leak into the main Prometheus module.
package arrowexp

import (
	"math/rand"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
)

// Sample is one float sample of one series, the unit the remote write queue
// manager works with.
type Sample struct {
	Labels labels.Labels
	T      int64
	V      float64
}

// VisitFunc is called by decoders for every decoded sample. The labels are
// only valid for the duration of the call, mirroring how the remote write
// handler passes them to an appender.
type VisitFunc func(ls labels.Labels, t int64, v float64)

var goRuntimeMetrics = []struct {
	name    string
	extra   []string
	counter bool
}{
	{"go_gc_duration_seconds", []string{"quantile", "0"}, false},
	{"go_gc_duration_seconds", []string{"quantile", "0.25"}, false},
	{"go_gc_duration_seconds", []string{"quantile", "0.5"}, false},
	{"go_gc_duration_seconds", []string{"quantile", "0.75"}, false},
	{"go_gc_duration_seconds", []string{"quantile", "1"}, false},
	{"go_gc_duration_seconds_sum", nil, true},
	{"go_gc_duration_seconds_count", nil, true},
	{"go_memstats_alloc_bytes_total", nil, true},
	{"go_memstats_frees_total", nil, true},
	{"go_memstats_lookups_total", nil, true},
	{"go_memstats_mallocs_total", nil, true},
	{"go_goroutines", nil, false},
	{"go_info", []string{"version", "go1.26.0"}, false},
	{"go_memstats_alloc_bytes", nil, false},
	{"go_memstats_buck_hash_sys_bytes", nil, false},
	{"go_memstats_gc_sys_bytes", nil, false},
	{"go_memstats_heap_alloc_bytes", nil, false},
	{"go_memstats_heap_idle_bytes", nil, false},
	{"go_memstats_heap_inuse_bytes", nil, false},
	{"go_memstats_heap_objects", nil, false},
	{"go_memstats_heap_released_bytes", nil, false},
	{"go_memstats_heap_sys_bytes", nil, false},
	{"go_memstats_last_gc_time_seconds", nil, false},
	{"go_memstats_mcache_inuse_bytes", nil, false},
	{"go_memstats_mcache_sys_bytes", nil, false},
	{"go_memstats_mspan_inuse_bytes", nil, false},
	{"go_memstats_mspan_sys_bytes", nil, false},
	{"go_memstats_next_gc_bytes", nil, false},
	{"go_memstats_other_sys_bytes", nil, false},
	{"go_memstats_stack_inuse_bytes", nil, false},
	{"go_memstats_stack_sys_bytes", nil, false},
	{"go_memstats_sys_bytes", nil, false},
	{"go_threads", nil, false},
}

// GenerateLowCardinality returns n samples resembling Go runtime metrics
// scraped from many Prometheus pods: few distinct label values, heavily
// repeated across series. Samples of one target share a scrape timestamp and
// counters carry integer values, as in real scrapes.
func GenerateLowCardinality(n int) []Sample {
	r := rand.New(rand.NewSource(0))
	const baseTS = int64(1_700_000_000_000)
	out := make([]Sample, 0, n)
	b := labels.NewBuilder(labels.EmptyLabels())
	for instance := 0; len(out) < n; instance++ {
		ts := baseTS + int64(instance%15)*1000
		for _, m := range goRuntimeMetrics {
			if len(out) == n {
				break
			}
			b.Reset(labels.EmptyLabels())
			b.Set("__name__", m.name)
			b.Set("cluster", "some-cluster-"+strconv.Itoa(instance%3))
			b.Set("container", "prometheus")
			b.Set("job", "some-namespace/prometheus")
			b.Set("namespace", "some-namespace")
			b.Set("pod", "prometheus-"+strconv.Itoa(instance))
			for i := 0; i < len(m.extra); i += 2 {
				b.Set(m.extra[i], m.extra[i+1])
			}
			v := r.Float64() * 1e6
			if m.counter {
				v = float64(r.Int63n(1 << 40))
			}
			out = append(out, Sample{Labels: b.Labels(), T: ts, V: v})
		}
	}
	return out
}

var kubeMetrics = []string{
	"kube_pod_info",
	"kube_pod_status_phase",
	"kube_pod_container_status_restarts_total",
	"kube_pod_container_resource_requests",
	"container_cpu_usage_seconds_total",
	"container_memory_working_set_bytes",
}

// GenerateHighCardinality returns n samples resembling kube-state-metrics and
// cAdvisor series: almost every series carries unique label values (pod name,
// pod UID, node, container ID), so symbols are rarely reused.
func GenerateHighCardinality(n int) []Sample {
	r := rand.New(rand.NewSource(1))
	const baseTS = int64(1_700_000_000_000)
	out := make([]Sample, 0, n)
	b := labels.NewBuilder(labels.EmptyLabels())
	for pod := 0; len(out) < n; pod++ {
		uid := strconv.FormatUint(r.Uint64(), 16) + "-" + strconv.FormatUint(r.Uint64(), 16)
		podName := "app-" + strconv.Itoa(pod/20) + "-" + strconv.FormatUint(r.Uint64()%1_000_000, 36)
		node := "ip-10-0-" + strconv.Itoa(pod%50) + "-" + strconv.Itoa(pod%200) + ".ec2.internal"
		containerID := "containerd://" + strconv.FormatUint(r.Uint64(), 16) + strconv.FormatUint(r.Uint64(), 16)
		ts := baseTS + int64(pod%30)*1000
		for _, name := range kubeMetrics {
			if len(out) == n {
				break
			}
			b.Reset(labels.EmptyLabels())
			b.Set("__name__", name)
			b.Set("cluster", "prod-eu-west-1")
			b.Set("container", "app")
			b.Set("container_id", containerID)
			b.Set("instance", "10.0."+strconv.Itoa(pod%250)+"."+strconv.Itoa(pod%200)+":8080")
			b.Set("job", "kube-state-metrics")
			b.Set("namespace", "team-"+strconv.Itoa(pod%40))
			b.Set("node", node)
			b.Set("pod", podName)
			b.Set("uid", uid)
			v := r.Float64() * 1e9
			switch name {
			case "kube_pod_status_phase":
				b.Set("phase", "Running")
				v = 1
			case "kube_pod_info":
				b.Set("created_by_kind", "ReplicaSet")
				b.Set("host_ip", "10.0."+strconv.Itoa(pod%50)+".1")
				v = 1
			case "kube_pod_container_resource_requests":
				b.Set("resource", "cpu")
				b.Set("unit", "core")
				v = 0.25
			case "kube_pod_container_status_restarts_total":
				v = float64(r.Intn(5))
			}
			out = append(out, Sample{Labels: b.Labels(), T: ts, V: v})
		}
	}
	return out
}
