package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/internal/utils"
	"github.com/olicesx/quic-go/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDial(t *testing.T) {
	t.Run("Dial", func(t *testing.T) {
		testDial(t,
			func(ctx context.Context, addr net.Addr) error {
				conn := newUPDConnLocalhost(t)
				_, err := Dial(ctx, conn, addr, &tls.Config{}, nil)
				return err
			},
			false,
		)
	})

	t.Run("DialEarly", func(t *testing.T) {
		testDial(t,
			func(ctx context.Context, addr net.Addr) error {
				conn := newUPDConnLocalhost(t)
				_, err := DialEarly(ctx, conn, addr, &tls.Config{}, nil)
				return err
			},
			false,
		)
	})

	t.Run("DialAddr", func(t *testing.T) {
		testDial(t,
			func(ctx context.Context, addr net.Addr) error {
				_, err := DialAddr(ctx, addr.String(), &tls.Config{}, nil)
				return err
			},
			true,
		)
	})

	t.Run("DialAddrEarly", func(t *testing.T) {
		testDial(t,
			func(ctx context.Context, addr net.Addr) error {
				_, err := DialAddrEarly(ctx, addr.String(), &tls.Config{}, nil)
				return err
			},
			true,
		)
	})
}

func testDial(t *testing.T,
	dialFn func(context.Context, net.Addr) error,
	shouldCloseConn bool,
) {
	server := newUPDConnLocalhost(t)

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() { errChan <- dialFn(ctx, server.LocalAddr()) }()

	_, addr, err := server.ReadFrom(make([]byte, 1500))
	require.NoError(t, err)
	require.True(t, areTransportsRunning())
	cancel()
	select {
	case err := <-errChan:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// The socket that the client used for dialing should be closed now.
	// Binding to the same address would error if the address was still in use.
	conn, err := net.ListenUDP("udp", addr.(*net.UDPAddr))
	if shouldCloseConn {
		require.NoError(t, err)
		defer conn.Close()
	} else {
		require.Error(t, err)
		if runtime.GOOS == "windows" {
			require.ErrorContains(t, err, "bind: Only one usage of each socket address")
		} else {
			require.ErrorContains(t, err, "address already in use")
		}
	}

	require.False(t, areTransportsRunning())
}

type failingConnIDGenerator struct {
	err error
}

func (g *failingConnIDGenerator) GenerateConnectionID() (protocol.ConnectionID, error) {
	return protocol.ConnectionID{}, g.err
}

func (g *failingConnIDGenerator) ConnectionIDLen() int { return 4 }

type closeCountingPacketConn struct {
	net.PacketConn
	closes   atomic.Int32
	deadline atomic.Int32
}

func (c *closeCountingPacketConn) Close() error {
	if c.closes.Add(1) > 1 {
		return errors.New("double close")
	}
	return c.PacketConn.Close()
}

func (c *closeCountingPacketConn) SetReadDeadline(t time.Time) error {
	c.deadline.Add(1)
	return c.PacketConn.SetReadDeadline(t)
}

func TestSingleUseTransportEarlyFailuresAndIdempotentClose(t *testing.T) {
	t.Run("validateConfig failure on single-use Transport stops listen", func(t *testing.T) {
		pc := &closeCountingPacketConn{PacketConn: newUPDConnLocalhost(t)}
		tr, err := setupTransport(pc, &tls.Config{}, false)
		require.NoError(t, err)
		_, err = tr.DialEarly(context.Background(), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}, &tls.Config{}, &Config{Versions: []Version{1234}})
		require.ErrorContains(t, err, "invalid QUIC version")
		require.False(t, areTransportsRunning())
		require.Zero(t, pc.closes.Load(), "external PacketConn must not be closed by single-use Transport")
		require.NoError(t, pc.Close())
	})

	t.Run("GenerateConnectionID failure on single-use Transport stops listen", func(t *testing.T) {
		pc := &closeCountingPacketConn{PacketConn: newUPDConnLocalhost(t)}
		tr := &Transport{
			Conn:                  pc,
			isSingleUse:           true,
			ConnectionIDGenerator: &failingConnIDGenerator{err: errors.New("cid failed")},
		}
		_, err := tr.DialEarly(context.Background(), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}, &tls.Config{}, nil)
		require.ErrorContains(t, err, "cid failed")
		require.False(t, areTransportsRunning())
		require.Zero(t, pc.closes.Load())
		require.NoError(t, pc.Close())
	})

	t.Run("DialEarly returns earlyConn then stops Transport on close or recreate", func(t *testing.T) {
		for _, closeViaRecreate := range []bool{false, true} {
			originalClientConnConstructor := newClientConnection
			t.Cleanup(func() { newClientConnection = originalClientConnConstructor })
			mockCtrl := gomock.NewController(t)
			mockConn := NewMockQUICConn(mockCtrl)
			earlyReady := make(chan struct{})
			close(earlyReady)
			releaseRun := make(chan struct{})

			mockConn.EXPECT().earlyConnReady().Return(earlyReady)
			mockConn.EXPECT().HandshakeComplete().Return(make(chan struct{}))
			mockConn.EXPECT().run().DoAndReturn(func() error {
				<-releaseRun
				if closeViaRecreate {
					return &errCloseForRecreating{nextPacketNumber: 1, nextVersion: protocol.Version1}
				}
				return errors.New("closed after early return")
			})
			mockConn.EXPECT().destroy(gomock.Any()).AnyTimes()

			newClientConnection = func(
				_ context.Context, _ sendConn, _ connRunner,
				_ protocol.ConnectionID, _ protocol.ConnectionID,
				_ ConnectionIDGenerator, _ *statelessResetter,
				_ *Config, _ *tls.Config, _ protocol.PacketNumber,
				_ bool, _ bool, _ *logging.ConnectionTracer,
				_ utils.Logger, _ protocol.Version,
			) quicConn {
				return mockConn
			}

			pc := &closeCountingPacketConn{PacketConn: newUPDConnLocalhost(t)}
			qc, err := DialEarly(context.Background(), pc, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}, &tls.Config{}, nil)
			newClientConnection = originalClientConnConstructor
			require.NoError(t, err)
			require.NotNil(t, qc)
			require.Eventually(t, areTransportsRunning, time.Second, time.Millisecond)

			close(releaseRun)
			deadline := time.Now().Add(time.Second)
			for areTransportsRunning() {
				if time.Now().After(deadline) {
					t.Fatal("single-use Transport listen goroutine leaked after DialEarly lifecycle ended")
				}
				time.Sleep(time.Millisecond)
			}
			require.Zero(t, pc.closes.Load(), "external PacketConn must remain owned by caller")
			require.NoError(t, pc.Close())
			mockCtrl.Finish()
		}
	})

	t.Run("concurrent and repeated Close on createdConn Transport closes Conn once", func(t *testing.T) {
		pc := &closeCountingPacketConn{PacketConn: newUPDConnLocalhost(t)}
		tr, err := setupTransport(pc, &tls.Config{}, true)
		require.NoError(t, err)
		require.NoError(t, tr.init(true))

		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(t, tr.Close())
			}()
		}
		wg.Wait()
		require.NoError(t, tr.Close())
		require.Equal(t, int32(1), pc.closes.Load())
		require.False(t, areTransportsRunning())
	})
}
