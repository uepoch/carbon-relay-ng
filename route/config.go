package route

import (
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
)

// baseCfgExtender is a function that takes a baseConfig and returns
// a configuration object that implements Config. This function may be
// the identity function, i.e., it may simply return its argument.
// This mechanism supports maintaining different configuration objects for
// different route types and creating a route-appropriate configuration object
// on a configuration change.
// The baseRoute object implements private methods (addDestination,
// delDestination, etc.) that create a new baseConfig object that reflects
// the configuration change. This baseConfig object is applicable to all
// route types. However, some route types, like ConsistentHashingRoute, have
// additional configuration and therefore have a distinct configuration object
// that embeds baseConfig (in the case of ConsistentHashingRoute, the object
// is consistentHashingConfig). This route-type-specific configuration also
// needs to be updated on a base configuration change (e.g., on a change
// affecting destinations). Accordingly, the public entry points that effect the
// configuration change (Add, DelDestination, etc.) are implemented for baseRoute
// and also for any route type, like ConsistentHashingRoute, that creates a
// configuration object. These public entry points call the private method,
// passing in a callback function of type baseCfgExtender, which takes a
// baseConfig and either returns it unchanged or creates an outer
// configuration object with the baseConfig embedded in it.
// The private method then stores the Config object returned by the callback.
type baseCfgExtender func(baseConfig) Config

func baseConfigExtender(baseConfig baseConfig) Config {
	return baseConfig
}

type Config interface {
	Matcher() *matcher.Matcher
	Dests() []*dest.Destination
}

type baseConfig struct {
	matcher matcher.Matcher
	dests   []*dest.Destination
}

func (c baseConfig) Matcher() *matcher.Matcher {
	return &c.matcher
}

func (c baseConfig) Dests() []*dest.Destination {
	return c.dests
}
