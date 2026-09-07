package quic

import (
	"expvar"
	"sync/atomic"
	"time"
)

// datagramWaitObservation samples one operation in 64. Buckets are disjoint and
// describe local waiting, not end-to-end latency or successful delivery.
// The snapshot is served by the existing localhost diagnostics listener.
type datagramWaitObservation struct {
	operations atomic.Uint64
	buckets    [7]atomic.Uint64
}

func (o *datagramWaitObservation) start() time.Time {
	if o.operations.Add(1)%64 != 0 {
		return time.Time{}
	}
	return time.Now()
}

func (o *datagramWaitObservation) finish(start time.Time) {
	if start.IsZero() {
		return
	}
	elapsed := time.Since(start)
	limits := [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, time.Second}
	i := 0
	for i < len(limits) && elapsed > limits[i] {
		i++
	}
	o.buckets[i].Add(1)
}

func (o *datagramWaitObservation) snapshot() map[string]any {
	counts := make([]uint64, len(o.buckets))
	for i := range counts {
		counts[i] = o.buckets[i].Load()
	}
	return map[string]any{"operations": o.operations.Load(), "sample_every": 64, "bucket_upper_ms": []string{"1", "5", "20", "50", "100", "1000", "+Inf"}, "samples": counts}
}

var datagramEnqueueWait, datagramQueueWait datagramWaitObservation
var datagramReceiveDrops, datagramPackerDrops atomic.Uint64

func init() {
	expvar.Publish("quic_datagram", expvar.Func(func() any {
		return map[string]any{"enqueue_wait": datagramEnqueueWait.snapshot(), "send_queue_wait": datagramQueueWait.snapshot(), "receive_queue_drops": datagramReceiveDrops.Load(), "packer_size_drops": datagramPackerDrops.Load()}
	}))
}
