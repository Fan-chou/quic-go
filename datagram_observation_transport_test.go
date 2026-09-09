package quic

import (
	"testing"
	"time"
)

func TestDatagramSlowTransportSample(t *testing.T) {
	var observation datagramWaitObservation
	calls := 0
	capture := func() *datagramTransportSample {
		calls++
		return &datagramTransportSample{Remote: "192.0.2.1:443", PendingAfterPop: 3}
	}
	observation.finishWithTransport(time.Time{}, capture)
	observation.finishWithTransport(time.Now(), capture)
	if calls != 0 {
		t.Fatal("captured transport for unsampled or fast operation")
	}
	observation.finishWithTransport(time.Now().Add(-150*time.Millisecond), capture)
	sample := observation.lastSlow.Load()
	if calls != 1 || sample == nil || sample.Transport == nil || sample.Transport.Remote != "192.0.2.1:443" || sample.Transport.PendingAfterPop != 3 {
		t.Fatalf("missing slow transport snapshot: %#v, calls=%d", sample, calls)
	}
}
