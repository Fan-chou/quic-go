package quic

import (
	"testing"

	"github.com/olicesx/quic-go/internal/ackhandler"
	"github.com/olicesx/quic-go/internal/wire"
)

type applicationLimitedHandler struct {
	ackhandler.SentPacketHandler
	calls int
}

func (h *applicationLimitedHandler) OnApplicationLimited() { h.calls++ }

func TestApplicationLimitedRequiresEmptyQueues(t *testing.T) {
	h := &applicationLimitedHandler{}
	f := newFramer(nil)
	s := &connection{sentPacketHandler: h, framer: f, retransmissionQueue: &retransmissionQueue{}}
	s.reportApplicationLimited()
	if h.calls != 1 {
		t.Fatal("empty supply was not reported")
	}
	f.QueueControlFrame(&wire.PingFrame{})
	s.reportApplicationLimited()
	if h.calls != 1 {
		t.Fatal("queued control data reported idle")
	}
}

func TestApplicationLimitedPendingDatagramAndRetransmission(t *testing.T) {
	h := &applicationLimitedHandler{}
	q := newDatagramQueue(func() {}, nil)
	q.sendQueue.PushBack(queuedDatagram{frame: &wire.DatagramFrame{Data: []byte{1}}})
	s := &connection{sentPacketHandler: h, framer: newFramer(nil), retransmissionQueue: &retransmissionQueue{}, datagramQueue: q}
	s.reportApplicationLimited()
	if h.calls != 0 {
		t.Fatal("queued DATAGRAM reported idle")
	}
	q.sendQueue.PopFront()
	s.retransmissionQueue.appData = []wire.Frame{&wire.PingFrame{}}
	s.reportApplicationLimited()
	if h.calls != 0 {
		t.Fatal("retransmission reported idle")
	}
	s.retransmissionQueue.appData = nil
	s.reportApplicationLimited()
	if h.calls != 1 {
		t.Fatal("drained queues did not report idle")
	}
}
