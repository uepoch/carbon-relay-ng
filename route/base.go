package route

import (
	"fmt"
	"sync"
	"sync/atomic"

	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/metrics"
	"go.uber.org/zap"
)

type baseRoute struct {
	sync.Mutex              // only needed for the multiple writers
	config     atomic.Value // for reading and writing

	key       string
	routeType string
	rm        *metrics.RouteMetrics
	destMap   map[string]*dest.Destination
	logger    *zap.Logger
}

func newBaseRoute(key, routeType string) *baseRoute {
	return &baseRoute{
		sync.Mutex{},
		atomic.Value{},
		key,
		routeType,
		metrics.NewRouteMetrics(key, routeType, nil),
		map[string]*dest.Destination{},
		zap.L().With(zap.String("routekey", key), zap.String("route_type", routeType)),
	}
}

// Add adds a new Destination to the Route and automatically runs it for you.
// The destination must not be running already!
func (route *baseRoute) addDestination(dest *dest.Destination, extendConfig baseCfgExtender) {
	route.Lock()
	defer route.Unlock()
	conf := route.config.Load().(Config)
	go dest.Run()
	newDests := append(conf.Dests(), dest)
	newConf := extendConfig(baseConfig{*conf.Matcher(), newDests})
	route.destMap[dest.Key] = dest
	route.config.Store(newConf)
}

func (route *baseRoute) Add(dest *dest.Destination) {
	route.addDestination(dest, baseConfigExtender)
}

func (route *baseRoute) delDestination(index int, extendConfig baseCfgExtender) error {
	route.Lock()
	defer route.Unlock()
	conf := route.config.Load().(Config)
	if index >= len(conf.Dests()) {
		return fmt.Errorf("Invalid index %d", index)
	}
	d := conf.Dests()[index]
	if err := d.Shutdown(); err != nil {
		route.logger.Warn("error while shutting down destination", zap.String("destination_key", d.Key))
	}
	newDests := append(conf.Dests()[:index], conf.Dests()[index+1:]...)
	newConf := extendConfig(baseConfig{*conf.Matcher(), newDests})
	delete(route.destMap, d.Key)
	route.config.Store(newConf)
	return nil
}

func (route *baseRoute) DelDestination(index int) error {
	return route.delDestination(index, baseConfigExtender)
}

func (route *baseRoute) GetDestinations() []*dest.Destination {
	conf := route.config.Load().(Config)
	return conf.Dests()
}

func (route *baseRoute) GetDestinationByName(destName string) (*dest.Destination, error) {
	route.Lock()
	defer route.Unlock()

	d, ok := route.destMap[destName]
	if !ok {
		return nil, fmt.Errorf("Destination not found %s", destName)
	}
	return d, nil
}

func (route *baseRoute) GetDestination(index int) (*dest.Destination, error) {
	route.Lock()
	defer route.Unlock()
	conf := route.config.Load().(Config)
	if index >= len(conf.Dests()) {
		return nil, fmt.Errorf("Invalid index %d", index)
	}
	return conf.Dests()[index], nil
}

func (route *baseRoute) update(opts map[string]string, extendConfig baseCfgExtender) error {
	route.Lock()
	defer route.Unlock()
	conf := route.config.Load().(Config)
	match := conf.Matcher()
	prefix := match.Prefix
	sub := match.Sub
	regex := match.Regex
	updateMatcher := false

	for name, val := range opts {
		switch name {
		case "prefix":
			prefix = val
			updateMatcher = true
		case "sub":
			sub = val
			updateMatcher = true
		case "regex":
			regex = val
			updateMatcher = true
		default:
			return fmt.Errorf("no such option '%s'", name)
		}
	}
	if updateMatcher {
		match, err := matcher.New(prefix, sub, regex)
		if err != nil {
			return err
		}
		conf = extendConfig(baseConfig{*match, conf.Dests()})
	}
	route.config.Store(conf)
	return nil
}

func (route *baseRoute) Update(opts map[string]string) error {
	return route.update(opts, baseConfigExtender)
}

func (route *baseRoute) updateDestination(index int, opts map[string]string, extendConfig baseCfgExtender) error {
	route.Lock()
	defer route.Unlock()
	conf := route.config.Load().(Config)
	if index >= len(conf.Dests()) {
		return fmt.Errorf("Invalid index %d", index)
	}
	err := conf.Dests()[index].Update(opts)
	if err != nil {
		return err
	}
	conf = extendConfig(baseConfig{*conf.Matcher(), conf.Dests()})
	route.config.Store(conf)
	return nil
}

func (route *baseRoute) UpdateDestination(index int, opts map[string]string) error {
	return route.updateDestination(index, opts, baseConfigExtender)
}

func (route *baseRoute) updateMatcher(matcher matcher.Matcher, extendConfig baseCfgExtender) {
	route.Lock()
	defer route.Unlock()
	conf := route.config.Load().(Config)
	conf = extendConfig(baseConfig{matcher, conf.Dests()})
	route.config.Store(conf)
}

func (route *baseRoute) UpdateMatcher(matcher matcher.Matcher) {
	route.updateMatcher(matcher, baseConfigExtender)
}

func (route *baseRoute) Key() string {
	return route.key
}

func (route *baseRoute) Type() string {
	return route.routeType
}

func (route *baseRoute) MatchString(s string) bool {
	conf := route.config.Load().(Config)
	return conf.Matcher().MatchString(s)
}

func (route *baseRoute) Match(s []byte) bool {
	conf := route.config.Load().(Config)
	return conf.Matcher().Match(s)
}

func (route *baseRoute) Flush() error {
	conf := route.config.Load().(Config)

	for _, d := range conf.Dests() {
		err := d.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (route *baseRoute) Shutdown() error {
	conf := route.config.Load().(Config)

	destErrs := make([]error, 0)

	for _, d := range conf.Dests() {
		err := d.Shutdown()
		if err != nil {
			destErrs = append(destErrs, err)
		}
	}

	if len(destErrs) == 0 {
		return nil
	}
	errStr := ""
	for _, e := range destErrs {
		errStr += "   " + e.Error()
	}
	return fmt.Errorf("one or more destinations failed to shutdown:" + errStr)
}

func (route *baseRoute) run() {
	for _, d := range route.GetDestinations() {
		d.Run()
	}
}

func (route *baseRoute) Snapshot() Snapshot {
	return makeSnapshot(route)
}
