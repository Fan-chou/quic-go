package ackhandler

import (
	"testing"

	"github.com/olicesx/quic-go/congestion"
)

type applicationSupplyCC struct {
	congestion.CongestionControl
	reports []bool
}

func (c *applicationSupplyCC) SetRTTStatsProvider(congestion.RTTStatsProvider) {}
func (c *applicationSupplyCC) SetApplicationLimited(v bool)                    { c.reports = append(c.reports, v) }
func TestApplicationLimitedAdapterNotification(t *testing.T) {
	c := &applicationSupplyCC{}
	h := &sentPacketHandler{}
	h.SetCongestionControl(c)
	h.OnApplicationLimited()
	if len(c.reports) != 2 || c.reports[0] || !c.reports[1] {
		t.Fatalf("reports = %v", c.reports)
	}
}
