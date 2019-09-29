package route

import (
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/encoding"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"go.uber.org/zap"
)

type SendFirstMatch struct {
	baseRoute
}

// NewSendFirstMatch creates a sendFirstMatch route.
// We will automatically run the route and the given destinations
func NewSendFirstMatch(key, prefix, sub, regex string, destinations []*dest.Destination) (Route, error) {
	m, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	r := &SendFirstMatch{*newBaseRoute(key, "SendFirstMatch", m)}
	r.config.Store(baseConfig{*m, destinations})
	r.run()
	return r, nil
}

func (route *SendFirstMatch) Dispatch(d encoding.Datapoint) {
	conf := route.config.Load().(Config)

	for _, dest := range conf.Dests() {
		if dest.MatchString(d.Name) {
			// dest should handle this as quickly as it can
			zap.L().Debug("route %s sending to dest %s: %v", zap.String("destinationKey", dest.Key), zap.Stringer("datapoint", d))
			dest.In <- d
			route.rm.OutMetrics.Inc()
			break
		}
	}
}
