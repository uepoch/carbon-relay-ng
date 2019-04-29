package discovery

import (
	"github.com/graphite-ng/carbon-relay-ng/discovery/consul"
)

type DiscoveryProvider struct {
	Consul *consul.ConsulProvider
}
