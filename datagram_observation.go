package quic

import (
	"expvar"
	"sync/atomic"
	"time"
)

// datagramWaitObservation samples one operation in 64. Buckets are disjoint and
// describe local waiting, not end-to-end latency or successful delivery.
// The snapshot is served by the existing localhost diagnostics listener.
type datagramSlowSample struct {
	At           string                   `json:"at"`
	Milliseconds float64                  `json:"milliseconds"`
	Transport    *datagramTransportSample `json:"transport,omitempty"`
}

// Captured on the connection send loop, never from the expvar reader.
// This is state at dequeue time, not proof of the cause of the preceding wait.
type datagramTransportSample struct {
	Local                   string  `json:"local"`
	Remote                  string  `json:"remote"`
	SmoothedRTTMilliseconds float64 `json:"smoothed_rtt_ms"`
	LatestRTTMilliseconds   float64 `json:"latest_rtt_ms"`
	PendingAfterPop         int     `json:"pending_after_pop"`
}

type datagramWaitObservation struct {
	operations atomic.Uint64
	lastSlow   atomic.Pointer[datagramSlowSample]
	buckets    [9]atomic.Uint64
}

func (o *datagramWaitObservation) start() time.Time {
	if o.operations.Add(1)%64 != 0 {
		return time.Time{}
	}
	return time.Now()
}

func (o *datagramWaitObservation) finish(start time.Time) {
	o.finishWithTransport(start, nil)
}

func (o *datagramWaitObservation) finishWithTransport(start time.Time, transport func() *datagramTransportSample) {
	if start.IsZero() {
		return
	}
	elapsed := time.Since(start)
	limits := [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 500 * time.Millisecond, time.Second}
	i := 0
	for i < len(limits) && elapsed > limits[i] {
		i++
	}
	o.buckets[i].Add(1)
	if elapsed >= 100*time.Millisecond {
		sample := &datagramSlowSample{At: time.Now().UTC().Format(time.RFC3339Nano), Milliseconds: float64(elapsed) / float64(time.Millisecond)}
		if transport != nil {
			sample.Transport = transport()
		}
		o.lastSlow.Store(sample)
	}
}

func (o *datagramWaitObservation) snapshot() map[string]any {
	counts := make([]uint64, len(o.buckets))
	for i := range counts {
		counts[i] = o.buckets[i].Load()
	}
	return map[string]any{"last_slow_sample": o.lastSlow.Load(), "operations": o.operations.Load(), "sample_every": 64, "bucket_upper_ms": []string{"1", "5", "20", "50", "100", "200", "500", "1000", "+Inf"}, "samples": counts}
}

var (
	datagramEnqueueWait, datagramQueueWait    datagramWaitObservation
	datagramReceiveDrops, datagramPackerDrops atomic.Uint64
	datagramSendLimited                       [3]atomic.Uint64
)

// These count scheduling decisions while DATAGRAMs are pending, not packets
// lost or time spent blocked. A packet can encounter more than one reason.
func (h *datagramQueue) observeSendLimit(reason int) {
	if h == nil {
		return
	}
	h.sendMx.Lock()
	pending := !h.sendQueue.Empty()
	h.sendMx.Unlock()
	if pending {
		datagramSendLimited[reason].Add(1)
	}
}

func init() {
	expvar.Publish("quic_datagram", expvar.Func(func() any {
		return map[string]any{"send_limit_events": map[string]uint64{"pacing": datagramSendLimited[0].Load(), "congestion": datagramSendLimited[1].Load(), "sender_queue": datagramSendLimited[2].Load()}, "enqueue_wait": datagramEnqueueWait.snapshot(), "send_queue_wait": datagramQueueWait.snapshot(), "receive_queue_drops": datagramReceiveDrops.Load(), "packer_size_drops": datagramPackerDrops.Load()}
	}))
}
