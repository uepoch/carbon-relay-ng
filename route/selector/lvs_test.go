package selector

import (
	"fmt"
	"testing"

	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"go.uber.org/zap"
)

func testingSelector(nodeNum int) *LVSSelector {
	s := NewLVSSelector(zap.NewNop())
	for i := 0; i < nodeNum; i++ {
		s.Add(&dest.Destination{Key: fmt.Sprintf("test-%d", i), Online: true}, 1)
	}
	return s
}

func BenchmarkLVSGetDest(b *testing.B) {
	s := testingSelector(150)
	b.ResetTimer()
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			s.GetDestination()
		}
	})
}
