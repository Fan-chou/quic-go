//go:build linux || darwin

package quic

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/internal/wire"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

func TestMsgSizeErrRecognition(t *testing.T) {
	wrapped := &net.OpError{
		Op:  "read",
		Net: "udp",
		Err: os.NewSyscallError("recvmmsg", unix.EMSGSIZE),
	}
	fmtWrapped := fmt.Errorf("outer: %w", unix.EMSGSIZE)

	require.True(t, isRecvMsgSizeErr(unix.EMSGSIZE))
	require.True(t, isRecvMsgSizeErr(wrapped))
	require.True(t, isRecvMsgSizeErr(fmtWrapped))
	require.False(t, isRecvMsgSizeErr(nil))
	require.False(t, isRecvMsgSizeErr(unix.ECONNRESET))
	require.False(t, isRecvMsgSizeErr(errors.New("other error")))

	require.True(t, isSendMsgSizeErr(unix.EMSGSIZE))
	require.True(t, isSendMsgSizeErr(wrapped))
	require.False(t, isSendMsgSizeErr(unix.ECONNRESET))
}

type mockListenRawConn struct {
	net.PacketConn
	readFn func() (receivedPacket, error)
}

func (c *mockListenRawConn) ReadPacket() (receivedPacket, error) {
	return c.readFn()
}

func (c *mockListenRawConn) WritePacket(b []byte, addr net.Addr, packetInfoOOB []byte, gsoSize uint16, ecn protocol.ECN) (int, error) {
	return len(b), nil
}

func (c *mockListenRawConn) capabilities() connCapabilities {
	return connCapabilities{DF: true}
}

func TestTransportListenIgnoresRecvEMSGSIZE(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	phm := NewMockPacketHandlerManager(mockCtrl)

	connID := protocol.ParseConnectionID([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	handled := make(chan receivedPacket, 1)
	phm.EXPECT().Get(connID).Return(&mockPacketHandler{packets: handled}, true)

	readErrs := make(chan error, 4)
	closed := make(chan struct{})
	var closeOnce sync.Once
	step := 0
	var mu sync.Mutex
	pktBytes := getPacket(t, connID)

	raw := &mockListenRawConn{
		PacketConn: &mockPacketConn{
			localAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234},
			readErrs:  readErrs,
		},
		readFn: func() (receivedPacket, error) {
			mu.Lock()
			s := step
			step++
			mu.Unlock()
			switch s {
			case 0:
				return receivedPacket{}, unix.EMSGSIZE
			case 1:
				return receivedPacket{}, &net.OpError{
					Op:  "recvmmsg",
					Net: "udp",
					Err: os.NewSyscallError("recvmmsg", unix.EMSGSIZE),
				}
			case 2:
				buf := getPacketBuffer()
				buf.Data = append(buf.Data[:0], pktBytes...)
				return receivedPacket{
					remoteAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5555},
					rcvTime:    time.Now(),
					data:       buf.Data,
					buffer:     buf,
				}, nil
			default:
				<-closed
				return receivedPacket{}, net.ErrClosed
			}
		},
	}

	tr := &Transport{Conn: raw, handlerMap: phm}
	require.NoError(t, tr.init(true))
	defer func() {
		phm.EXPECT().Close(gomock.Any())
		closeOnce.Do(func() { close(closed) })
		require.NoError(t, tr.Close())
	}()

	select {
	case p := <-handled:
		parsed, err := wire.ParseConnectionID(p.data, 0)
		require.NoError(t, err)
		require.Equal(t, connID, parsed)
		p.buffer.Release()
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for packet after EMSGSIZE")
	}

	// Transport must still be open and usable.
	tr.mutex.Lock()
	closeErr := tr.closeErr
	tr.mutex.Unlock()
	require.NoError(t, closeErr)
}

type emsgsizeBatchConn struct {
	calls int
}

func (b *emsgsizeBatchConn) ReadBatch(ms []ipv4.Message, _ int) (int, error) {
	b.calls++
	switch b.calls {
	case 1:
		// Partial batch (1 of batchSize messages)
		data := []byte("first")
		copy(ms[0].Buffers[0], data)
		ms[0].N = len(data)
		ms[0].NN = 0
		return 1, nil
	case 2:
		return 0, unix.EMSGSIZE
	case 3:
		data := []byte("after-emsgsize")
		copy(ms[0].Buffers[0], data)
		ms[0].N = len(data)
		ms[0].NN = 0
		return 1, nil
	default:
		return 0, net.ErrClosed
	}
}

func TestOOBConnReadPacketRetriesCleanlyAfterEMSGSIZE(t *testing.T) {
	udpConn := newUPDConnLocalhost(t)
	oc, err := newConn(udpConn, true)
	require.NoError(t, err)
	bc := &emsgsizeBatchConn{}
	oc.batchConn = bc

	p1, err := oc.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte("first"), p1.data)
	p1.buffer.Release()

	_, err = oc.ReadPacket()
	require.ErrorIs(t, err, unix.EMSGSIZE)

	p2, err := oc.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte("after-emsgsize"), p2.data)
	p2.buffer.Release()
	require.Equal(t, 3, bc.calls)
}

func TestHandleCloseErrorIgnoresSendConnectionCloseEMSGSIZE(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	tc := newServerTestConnection(t, mockCtrl, nil, false)
	tc.conn.sentFirstPacket = true

	// Mock packing a 1-RTT CONNECTION_CLOSE packet and returning EMSGSIZE on sync Write.
	tc.packer.EXPECT().PackApplicationClose(gomock.Any(), gomock.Any(), gomock.Any()).Return(&coalescedPacket{
		buffer: getPacketWithContents([]byte("close-pkt")),
	}, nil)
	tc.sendConn.EXPECT().Write([]byte("close-pkt"), uint16(0), gomock.Any()).Return(unix.EMSGSIZE)
	tc.connRunner.EXPECT().ReplaceWithClosed(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(_ []protocol.ConnectionID, pkt []byte, _ net.Addr) {
		require.Equal(t, []byte("close-pkt"), pkt)
	})
	tc.connRunner.EXPECT().Remove(gomock.Any()).AnyTimes()

	tc.conn.handleCloseError(&closeError{
		err: &ApplicationError{ErrorCode: 42, ErrorMessage: "bye"},
	})
}
