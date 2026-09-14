package ackhandler

import (
	"testing"

	"github.com/olicesx/quic-go/congestion"
	"github.com/olicesx/quic-go/internal/protocol"
)

type applicationSupplyCC struct {
	congestion.CongestionControl
	reports []bool
	window  congestion.ByteCount
}

func (c *applicationSupplyCC) SetRTTStatsProvider(congestion.RTTStatsProvider) {}
func (c *applicationSupplyCC) SetApplicationLimited(v bool)                    { c.reports = append(c.reports, v) }
func (c *applicationSupplyCC) GetCongestionWindow() congestion.ByteCount       { return c.window }
func TestApplicationLimitedAdapterNotification(t *testing.T) {
	c := &applicationSupplyCC{window: 12000}
	h := &sentPacketHandler{}
	h.SetCongestionControl(c)
	h.OnApplicationLimited()
	if len(c.reports) != 2 || c.reports[0] || !c.reports[1] {
		t.Fatalf("reports = %v", c.reports)
	}
}

func TestApplicationLimitedDoesNotMaskFullCongestionWindow(t *testing.T) {
	c := &applicationSupplyCC{window: 12000}
	h := &sentPacketHandler{}
	h.SetCongestionControl(c)
	c.reports = nil
	for _, flight := range []int64{12000, 13000} {
		h.bytesInFlight = protocol.ByteCount(flight)
		h.OnApplicationLimited()
	}
	if len(c.reports) != 0 {
		t.Fatalf("congestion-limited reports: %v", c.reports)
	}
	h.bytesInFlight = 11999
	h.OnApplicationLimited()
	if len(c.reports) != 1 || !c.reports[0] {
		t.Fatalf("application-limited report lost: %v", c.reports)
	}
}
