package route

import (
	"github.com/graphite-ng/carbon-relay-ng/encoding"

	dest "github.com/graphite-ng/carbon-relay-ng/destination"
)

// Route is a sinkhole for metrics, matching and forwarding points accordingly
type Route interface {
	Dispatch(encoding.Datapoint)
	Match(s []byte) bool
	MatchString(s string) bool
	Snapshot() Snapshot
	Key() string
	Type() string
	Flush() error
	Shutdown() error
	GetDestination(index int) (*dest.Destination, error)
	GetDestinations() []*dest.Destination
	DelDestination(index int) error
	UpdateDestination(index int, opts map[string]string) error
	Update(opts map[string]string) error
}
