package route

import (
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/encoding"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/route/selector"
	"go.uber.org/zap"
)

type LoadBalance struct {
	baseRoute
	selector *selector.LVSSelector
}

// NewSendFirstMatch creates a sendFirstMatch route.
// We will automatically run the route and the given destinations
func NewLoadBalance(key string, matcher *matcher.Matcher, destinations []*dest.Destination) (Route, error) {
	r := &LoadBalance{
		baseRoute: *newBaseRoute(key, "loadbalancing", *matcher),
	}

	s := selector.NewLVSSelector(r.logger)
	r.selector = s
	for _, d := range destinations {
		r.Add(d)
	}

	return r, nil
}

func (route *LoadBalance) DelDestination(i int) error {
	d, err := route.GetDestination(i)
	if err != nil {
		return err
	}
	if err = route.baseRoute.DelDestination(i); err != nil {
		return err
	}
	route.selector.DelDestination(d)
	return nil
}

func (route *LoadBalance) Add(d *dest.Destination) {
	route.selector.Add(d, 1)
	route.baseRoute.Add(d)
}

func (route *LoadBalance) Dispatch(d encoding.Datapoint) {
	if dest := route.selector.GetDestination(); dest != nil {
		route.logger.Debug("sending to dest", zap.String("destination_key", dest.Key), zap.Stringer("datapoint", d))
		dest.In <- d
		route.rm.OutMetrics.Inc()
	} else {
		route.logger.Warn("unable to send point: no destination available")
		route.rm.Errors.WithLabelValues("no destinations").Inc()
	}
}
