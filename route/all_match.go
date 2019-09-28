package route

import (
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/encoding"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"go.uber.org/zap"
)

type SendAllMatch struct {
	baseRoute
}

// NewSendAllMatch creates a sendAllMatch route.
// We will automatically run the route and the given destinations
func NewSendAllMatch(key, prefix, sub, regex string, destinations []*dest.Destination) (Route, error) {
	m, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	r := &SendAllMatch{*newBaseRoute(key, "SendAllMatch")}
	r.config.Store(baseConfig{*m, destinations})
	r.run()
	return r, nil
}

// Dispatch will process a datapoint, sending it to all matching destinations
func (route *SendAllMatch) Dispatch(d encoding.Datapoint) {
	conf := route.config.Load().(Config)

	for _, dest := range conf.Dests() {
		if dest.MatchString(d.Name) {
			// dest should handle this as quickly as it can
			route.logger.Debug("route sending to dest", zap.String("destinationKey", dest.Key), zap.Stringer("datapoint", d))
			dest.In <- d
			route.rm.OutMetrics.Inc()
		}
	}
}
