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

func TestDistribution(t *testing.T) {
	type test struct {
		dests []*weightedDest
	}
	tests := []test{
		{[]*weightedDest{
			{Dest: &dest.Destination{Key: "foo", Online: true}, Weight: 1},
			{Dest: &dest.Destination{Key: "bar", Online: true}, Weight: 2},
			{Dest: &dest.Destination{Key: "foobar", Online: true}, Weight: 3},
			{Dest: &dest.Destination{Key: "offline", Online: false}, Weight: 3},
		}},
	}

	for _, test := range tests {
		s := testingSelector(0)
		count := 0
		for _, d := range test.dests {
			s.Add(d.Dest, d.Weight)
			d.Weight *= 3
			if d.Dest.Online {
				count += d.Weight
			}
		}
		for i := 0; i < count; i++ {
			target := s.GetDestination()
			for _, d := range test.dests {
				if d.Dest == target {
					d.Weight -= 1
					break
				}
			}
		}

		for _, d := range test.dests {
			if d.Weight != 0 && d.Dest.Online {
				t.Errorf("destination %s have positive remaining counts, expected 0 : %d", d.Dest.Key, d.Weight)
			}
		}
	}
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
