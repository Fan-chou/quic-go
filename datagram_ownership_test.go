package quic

import (
	"context"
	"reflect"
	"testing"

	"github.com/olicesx/quic-go/internal/wire"
)

// TestReceiveQueueStorageBoundedWithoutFullDrain pins the ring-buffer
// conversion: consuming entries must release their slots even while the queue
// never fully drains, so the underlying storage stays at the initialized
// capacity across many receive/release/enqueue cycles.
func TestReceiveQueueStorageBoundedWithoutFullDrain(t *testing.T) {
	q := newDatagramQueue(nil, nil)
	defer q.CloseWithError(nil)
	enqueue := func() { q.HandleDatagramFrame(&wire.DatagramFrame{Data: make([]byte, 64)}) }

	// Seed two entries, then run many cycles that keep at least one entry
	// queued at every enqueue (the old slice+head scheme retained every
	// consumed header in this pattern, growing without bound).
	enqueue()
	enqueue()
	for i := 0; i < 20000; i++ {
		data, err := q.Receive(context.Background())
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		q.ReleaseDatagram(data)
		enqueue() // never lets the queue drain empty
		if q.rcvQueue.Len() != 2 {
			t.Fatalf("iteration %d: queue len = %d, want 2", i, q.rcvQueue.Len())
		}
	}
	if n := reflect.ValueOf(q.rcvQueue).FieldByName("ring").Len(); n != maxDatagramRcvQueueLen {
		t.Fatalf("storage grew to %d", n)
	}
	// Drain in FIFO order.
	for i := 0; i < 2; i++ {
		if _, err := q.Receive(context.Background()); err != nil {
			t.Fatalf("final Receive %d: %v", i, err)
		}
	}
	if !q.rcvQueue.Empty() {
		t.Fatal("queue must be empty after drain")
	}
}

// The packet packer and tracing callback may retain the frame after dequeue.
// Closing the queue must not recycle or reset a frame already handed to them.
func TestDatagramSendFrameSurvivesDequeueAndClose(t *testing.T) {
	q := newDatagramQueue(func() {}, nil)
	f := &wire.DatagramFrame{DataLenPresent: true, Data: []byte("retained payload")}
	if err := q.Add(f); err != nil {
		t.Fatal(err)
	}
	held := q.Peek()
	q.Pop()
	q.CloseWithError(nil)
	if !held.DataLenPresent || string(held.Data) != "retained payload" {
		t.Fatalf("dequeued frame changed: %+v", held)
	}
}

func BenchmarkDatagramSendOwnedFrame(b *testing.B) {
	q := newDatagramQueue(func() {}, nil)
	defer q.CloseWithError(nil)
	payload := make([]byte, 1200)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := &wire.DatagramFrame{DataLenPresent: true, Data: append([]byte(nil), payload...)}
		if err := q.Add(f); err != nil {
			b.Fatal(err)
		}
		held := q.Peek()
		q.Pop()
		if len(held.Data) != len(payload) {
			b.Fatal("payload changed")
		}
	}
}
