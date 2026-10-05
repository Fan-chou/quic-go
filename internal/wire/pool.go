package wire

import (
	"sync"

	"github.com/olicesx/quic-go/internal/protocol"
)

var datagramFramePool = sync.Pool{
	New: func() any {
		return &DatagramFrame{
			Data:     make([]byte, 0, protocol.MaxPacketBufferSize),
			fromPool: true,
		}
	},
}

var streamFramePool = sync.Pool{
	New: func() any {
		return &StreamFrame{
			Data:     make([]byte, 0, protocol.MaxPacketBufferSize),
			fromPool: true,
		}
	},
}

func GetStreamFrame() *StreamFrame {
	f := streamFramePool.Get().(*StreamFrame)
	// Re-arm pool ownership: putStreamFrame clears it when a frame is
	// returned, so a recycled frame always starts out as pool-owned.
	f.fromPool = true
	return f
}

// GetDatagramFrame returns a DatagramFrame from the shared pool. The frame's
// Data buffer has capacity protocol.MaxPacketBufferSize and must be re-sliced
// before use. Return the frame with PutDatagramFrame once it has been packed.
func GetDatagramFrame() *DatagramFrame {
	f := datagramFramePool.Get().(*DatagramFrame)
	f.fromPool = true
	return f
}

// PutDatagramFrame returns a pooled DatagramFrame and its Data buffer to the
// pool. Frames not originating from the pool are ignored. Ownership is
// consumed: a second PutDatagramFrame for the same frame is a no-op unless the
// frame was handed out again in between, so a stray double-return of a frame
// still held by the caller cannot put the same pointer into the pool twice.
func PutDatagramFrame(f *DatagramFrame) {
	if !f.fromPool {
		return
	}
	if cap(f.Data) != protocol.MaxPacketBufferSize {
		return
	}
	f.fromPool = false
	f.Data = f.Data[:0]
	f.DataLenPresent = false
	datagramFramePool.Put(f)
}

func putStreamFrame(f *StreamFrame) {
	if !f.fromPool {
		return
	}
	if cap(f.Data) != protocol.MaxPacketBufferSize {
		panic("wire.PutStreamFrame called with packet of wrong size!")
	}
	// Consume ownership before the hand-back. Clearing the flag here makes a
	// back-to-back double PutBack a no-op instead of pooling the same pointer
	// twice (two owners, one Data buffer), and lets the connection-layer
	// tests assert exactly-once release on the frame they handed in. Fields
	// are reset so a future GetStreamFrame caller that forgets one cannot
	// inherit stale values, mirroring PutDatagramFrame.
	f.fromPool = false
	f.Data = f.Data[:0]
	f.StreamID = 0
	f.Offset = 0
	f.Fin = false
	f.DataLenPresent = false
	streamFramePool.Put(f)
}
