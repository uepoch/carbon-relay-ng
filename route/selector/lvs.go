package selector

import (
	"sync"

	"go.uber.org/zap"

	dest "github.com/graphite-ng/carbon-relay-ng/destination"
)

// LVSSelector implements http://kb.linuxvirtualserver.org/wiki/Weighted_Round-Robin_Scheduling
type weightedDest struct {
	Dest   *dest.Destination
	Weight int
}

type LVSSelector struct {
	sync.RWMutex
	dests      []*weightedDest
	n          int
	gcd        int
	i          int
	maxWeight  int
	currWeight int
	logger     *zap.Logger

	// 	smoothing      float64
	// updateInterval time.Duration
	// ctx        context.Context
}

func NewLVSSelector(logger *zap.Logger) *LVSSelector {
	return &LVSSelector{
		RWMutex: sync.RWMutex{},
		dests:   []*weightedDest{},
		logger:  logger,
	}
}

// TODO: Add a slow start logic
// func (s *LVSSelector) updateDestinations() {
// 	s.RLock()
// 	shouldUpdate := make([]*weightedDest, 0, len(s.dests))
// 	for _, d := range s.dests {
// 		if d.Dest.Online {
// 			shouldUpdate = append(shouldUpdate, d)
// 		}
// 	}
// 	s.RUnlock()
// }

// func (s *LVSSelector) Start() {
// 	go func() {
// 		s.logger.Debug("Starting selector update routine")
// 		tick := time.NewTicker(s.updateInterval)
// 		defer tick.Stop()
// 		for {
// 			select {
// 			case <-s.ctx.Done():
// 				return
// 			case <-tick.C:
// 				s.logger.Debug("updating destinations")
// 				s.updateDestinations()
// 			}
// 		}
// 	}()
// }

func (s *LVSSelector) remove(d *dest.Destination) {
	pos := 0
	newMaxWeight := 0
	newGCD := 0
	for i, internalDest := range s.dests {
		if d == internalDest.Dest {
			pos = i
		} else {
			if newGCD == 0 {
				newGCD = internalDest.Weight
			} else {
				newGCD = gcd(newGCD, internalDest.Weight)
			}
			if newMaxWeight < internalDest.Weight {
				newMaxWeight = internalDest.Weight
			}
		}
	}
	s.maxWeight = newMaxWeight
	s.gcd = newGCD
	s.i = -1
	s.currWeight = 0
	copy(s.dests[pos:], s.dests[pos+1:s.n+2])
	s.dests = s.dests[:s.n+1]
}

func (s *LVSSelector) DelDestination(d *dest.Destination) {
	s.Lock()
	defer s.Unlock()
	s.remove(d)
}

func (s *LVSSelector) Add(d *dest.Destination, weight int) {
	s.Lock()
	defer s.Unlock()
	if weight <= 0 {
		s.logger.Panic("weight can't be 0 or negative")
	}
	new := &weightedDest{Dest: d, Weight: weight}
	if s.gcd == 0 {
		s.gcd = weight
		s.maxWeight = weight
		s.i = -1
		s.currWeight = 0
	} else {
		s.gcd = gcd(s.gcd, weight)
		if s.maxWeight < weight {
			s.maxWeight = weight
		}
	}
	s.dests = append(s.dests, new)
	s.n++
}

func (s *LVSSelector) GetDestination() *dest.Destination {
	s.Lock()
	defer s.Unlock()
	if s.n == 0 {
		return nil
	}
	if s.n == 1 {
		return s.dests[0].Dest
	}
	passed := false
	next := (s.i + 1) %s.n
	for i := next;!passed || i != next ; i = (i+1)%s.n {
		passed = true
		if i == 0 {
			// Lower the weight to select lower nodes
			s.currWeight = s.currWeight - s.gcd
			if s.currWeight <= 0 {
				// reset the current weight as we don't have anything left
				s.currWeight = s.maxWeight
			}
		}

		if s.dests[i].Dest.Online && s.dests[i].Weight >= s.currWeight {
			s.i = i
			return s.dests[i].Dest
		}
	}
	return nil
}

func gcd(x, y int) int {
	var t int
	for {
		t = (x % y)
		if t > 0 {
			x = y
			y = t
		} else {
			return y
		}
	}
}
