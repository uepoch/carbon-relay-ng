package consul

import (
	"sync"

	"github.com/graphite-ng/carbon-relay-ng/util"

	"github.com/graphite-ng/carbon-relay-ng/discovery/change"
	"github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"
)

type ConsulService struct {
	sync.Mutex
	Service      string
	Tags         []string
	ServiceMetas map[string]string
	NodeMetas    map[string]string
	UnHealthy    bool
	changeCh     chan<- change.Change
	// shutDownCh <-chan struct{}

	currentServices []string
}

func NewConsulService(service string, tags []string, serviceMetas, nodeMetas map[string]string, unhealthy bool) *ConsulService {
	return &ConsulService{sync.Mutex{}, service, tags, serviceMetas, nodeMetas, unhealthy, make(chan<- change.Change), []string{}}
}

func (s *ConsulService) close() {
	close(s.changeCh)
}

func (s *ConsulService) process(entries []*api.ServiceEntry) error {
	matchingEntries := []string{}
	for _, se := range entries {
		if !util.SliceEquals(s.Tags, se.Service.Tags) {
			// Drop if not matching tags
			continue
		}
		if !mapMatch(se.Service.Meta, s.ServiceMetas) || !mapMatch(se.Node.Meta, s.NodeMetas) {
			continue
		}
		if se.Service.Service != s.Service {
			logrus.Errorf("mismatch between received service entry and watched one: %s | %s", s.Service, se.Service.Service)
			continue
		}
		for _, c := range se.Checks {
			switch c.Status {
			case api.HealthPassing:
			case api.HealthAny:
			case api.HealthWarning:
				// TODO: Implement proper weight reduction
			case api.HealthCritical:
				if !s.UnHealthy {
					// We stop here
					continue
				}
			default:
				// Unknown...
				logrus.Error("unknown status `%s` for check %s for service %s from consul", c.Status, c.Name, se.Service.Service)
				continue
			}
		}
		matchingEntries = append(matchingEntries, se.Service.Address)
	}

	s.Lock()
	defer s.Unlock()
	added, missing := util.SliceDiff(s.currentServices, matchingEntries)
	for _, todel := range missing {
		s.changeCh <- change.Change{
			Type:    change.Remove,
			Address: todel,
		}
	}
	for _, toadd := range added {
		s.changeCh <- change.Change{
			Type:    change.Add,
			Address: toadd,
		}
	}
	return nil
}

func mapMatch(haystack, needle map[string]string) bool {
	if (haystack == nil && needle != nil) || (haystack != nil && needle == nil) {
		return false
	}
	for k, v := range needle {
		if vv, ok := haystack[k]; !ok || vv != v {
			return false
		}
	}
	return true
}
