package route

import (
	"fmt"
	"testing"

	dest "github.com/graphite-ng/carbon-relay-ng/destination"
)

func nullerCh() chan *dest.Destination {
	r := make(chan *dest.Destination, 100)
	go func() {
		for _ = range r {
		}
	}()
}

func testingLBRoute(nodeNum int) *LoadBalance {
	dests := make([]*dest.Destination, 0, nodeNum)
	for i := 0; i < nodeNum; i++ {
		dests = append(dests, &dest.Destination{Online: true, Key: fmt.Sprintf("test-%d", i)})
	}

	return
}

func BenchmarkLoadBalancer(b *testing.B) {
	NewLoadBalance("", nil)
}
