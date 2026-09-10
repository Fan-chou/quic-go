package ackhandler

// DatagramSendState is called only on the connection's send loop. The returned
// map contains copied scalar values, so expvar never reads live controller state.
func (h *sentPacketHandler) DatagramSendState() map[string]any {
	cc := h.getCongestionControl()
	state := map[string]any{
		"cwnd_bytes":       cc.GetCongestionWindow(),
		"in_flight_bytes":  h.bytesInFlight,
		"sent_bytes":       h.bytesSent,
		"ack_only_packets": h.ackOnlyPackets,
		"ack_only_bytes":   h.ackOnlyBytes,
		"recovery":         cc.InRecovery(),
	}
	if adapter, ok := cc.(*ccAdapter); ok {
		if observer, ok := adapter.CC.(interface{ DatagramSendState() map[string]any }); ok {
			state["controller"] = observer.DatagramSendState()
		}
	}
	return state
}
