package route

// DispatchNonBlocking will dispatch in to buf.
// if buf is full, will discard the data
func dispatchNonBlocking(buf chan []byte, in []byte) {
	select {
	case buf <- in:
		routeBufferedMetricsGauge.WithLabelValues("all").Inc()
	default:
		routeErrCounter.WithLabelValues("all", "buffer_full").Inc()
	}
}

// DispatchBlocking will dispatch in to buf.
// If buf is full, the call will block
// note that in this case, numBuffered will contain size of buffer + number of waiting entries,
// and hence could be > bufSize
func dispatchBlocking(buf chan []byte, in []byte) {
	routeBufferedMetricsGauge.WithLabelValues("all").Inc()
	buf <- in
}
