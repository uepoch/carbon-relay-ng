package route

import (
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
)

// to view the state of the table/route at any point in time
func makeSnapshot(route *baseRoute) Snapshot {
	conf := route.config.Load().(Config)
	dests := make([]*dest.Destination, len(conf.Dests()))
	for i, d := range conf.Dests() {
		dests[i] = d.Snapshot()
	}
	return Snapshot{Matcher: *conf.Matcher(), Dests: dests, Type: route.routeType, Key: route.key}
}

type Snapshot struct {
	Matcher matcher.Matcher     `json:"matcher"`
	Dests   []*dest.Destination `json:"destination"`
	Type    string              `json:"type"`
	Key     string              `json:"key"`
	Addr    string              `json:"addr,omitempty"`
}
