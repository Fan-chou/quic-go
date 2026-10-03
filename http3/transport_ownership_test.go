package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/olicesx/quic-go"
)

type ownedTestConn struct {
	quic.EarlyConnection
	closed atomic.Int32
}

func (*ownedTestConn) Context() context.Context { return context.Background() }
func (*ownedTestConn) HandshakeComplete() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (c *ownedTestConn) CloseWithError(quic.ApplicationErrorCode, string) error {
	c.closed.Add(1)
	return nil
}

type ownedTestClient struct {
	clientConn
	trip func(*http.Request) (*http.Response, error)
}

func (c ownedTestClient) RoundTrip(r *http.Request) (*http.Response, error) { return c.trip(r) }

func cachedOwnedClient(trip func(*http.Request) (*http.Response, error)) (*Transport, *ownedTestConn, *roundTripperWithCount) {
	conn := &ownedTestConn{}
	ch := make(chan struct{})
	close(ch)
	cl := &roundTripperWithCount{conn: conn, clientConn: ownedTestClient{trip: trip}, dialing: ch, cancel: func() {}}
	return &Transport{clients: map[string]*roundTripperWithCount{"example.com:443": cl}}, conn, cl
}

func TestTransportRetainsStreamErrorConnectionOwnership(t *testing.T) {
	tr, conn, cl := cachedOwnedClient(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("stream-local response error")
	})
	req, _ := http.NewRequest("GET", "https://example.com", nil)
	for range 2 {
		if _, err := tr.doRoundTripOpt(req, RoundTripOpt{}, false); err == nil {
			t.Fatal("expected stream error")
		}
		if tr.clients["example.com:443"] != cl || conn.closed.Load() != 0 {
			t.Fatal("stream error retired a usable connection")
		}
	}
	if err := tr.Close(); err != nil || conn.closed.Load() != 1 {
		t.Fatalf("lost close responsibility: closed=%d err=%v", conn.closed.Load(), err)
	}
}

func TestTransportRetiredConnectionWaitsForResponseBody(t *testing.T) {
	for _, forceClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "transport close"}[forceClose], func(t *testing.T) {
			tr, conn, cl := cachedOwnedClient(func(*http.Request) (*http.Response, error) {
				return &http.Response{Body: io.NopCloser(strings.NewReader("tail"))}, nil
			})
			req, _ := http.NewRequest("GET", "https://example.com", nil)
			rsp, err := tr.doRoundTripOpt(req, RoundTripOpt{}, false)
			if err != nil {
				t.Fatal(err)
			}
			tr.CloseIdleConnections()
			if conn.closed.Load() != 0 {
				t.Fatal("idle cleanup closed an active response")
			}
			cl.clientConn = ownedTestClient{trip: func(*http.Request) (*http.Response, error) {
				return nil, &errConnUnusable{e: errGoAway}
			}}
			// A second request observes GOAWAY while the first still has a body.
			_, _ = tr.doRoundTripOpt(req, RoundTripOpt{}, true)
			if conn.closed.Load() != 0 || len(tr.retired) != 1 {
				t.Fatal("retirement interrupted an active response")
			}
			if forceClose {
				if err := tr.Close(); err != nil {
					t.Fatal(err)
				}
				if conn.closed.Load() != 1 {
					t.Fatal("Close missed a retired connection")
				}
			}
			data, err := io.ReadAll(rsp.Body)
			if err != nil || string(data) != "tail" {
				t.Fatalf("tail lost: %q %v", data, err)
			}
			_ = rsp.Body.Close()
			if conn.closed.Load() != 1 || len(tr.retired) != 0 {
				t.Fatal("drained connection not closed exactly once")
			}
		})
	}
}

func TestTransportRetirementChecksConnectionIdentity(t *testing.T) {
	tr, oldConn, old := cachedOwnedClient(nil)
	_, newConn, replacement := cachedOwnedClient(nil)
	tr.clients["example.com:443"] = replacement
	old.useCount.Add(1)
	tr.retireClient("example.com:443", old)
	tr.releaseClient(old)
	if tr.clients["example.com:443"] != replacement || oldConn.closed.Load() != 1 || newConn.closed.Load() != 0 {
		t.Fatal("retiring an old request removed its replacement")
	}
	_ = tr.Close()
}

type countedBody struct {
	io.Reader
	closed atomic.Int32
}

func (b *countedBody) Close() error { b.closed.Add(1); return nil }

func TestTransportClosesReplayBodyOnRedialFailure(t *testing.T) {
	tr, _, _ := cachedOwnedClient(func(*http.Request) (*http.Response, error) {
		return nil, &Error{ErrorCode: ErrCodeRequestRejected}
	})
	tr.QUICConfig = &quic.Config{}
	tr.Dial = func(context.Context, string, *tls.Config, *quic.Config) (quic.EarlyConnection, error) {
		return nil, errors.New("redial failed")
	}
	replayed := &countedBody{Reader: strings.NewReader("request")}
	req, _ := http.NewRequest("POST", "https://example.com", io.NopCloser(strings.NewReader("request")))
	req.GetBody = func() (io.ReadCloser, error) { return replayed, nil }
	if _, err := tr.RoundTrip(req); err == nil {
		t.Fatal("redial must fail")
	}
	if replayed.closed.Load() != 1 {
		t.Fatalf("replay body closes=%d", replayed.closed.Load())
	}
	_ = tr.Close()
}

func TestTransportRejectedRetryDoesNotCreateThirdBody(t *testing.T) {
	reject := func(*http.Request) (*http.Response, error) { return nil, &Error{ErrorCode: ErrCodeRequestRejected} }
	tr, _, _ := cachedOwnedClient(reject)
	tr.QUICConfig = &quic.Config{}
	tr.Dial = func(context.Context, string, *tls.Config, *quic.Config) (quic.EarlyConnection, error) {
		return &ownedTestConn{}, nil
	}
	tr.newClientConn = func(quic.EarlyConnection) clientConn { return ownedTestClient{trip: reject} }
	var copies int
	replayed := &countedBody{Reader: strings.NewReader("request")}
	req, _ := http.NewRequest("POST", "https://example.com", io.NopCloser(strings.NewReader("request")))
	req.GetBody = func() (io.ReadCloser, error) { copies++; return replayed, nil }
	if _, err := tr.RoundTrip(req); err == nil {
		t.Fatal("both requests rejected")
	}
	if copies != 1 || replayed.closed.Load() != 1 {
		t.Fatalf("GetBody calls=%d, replay closes=%d", copies, replayed.closed.Load())
	}
	_ = tr.Close()
}
