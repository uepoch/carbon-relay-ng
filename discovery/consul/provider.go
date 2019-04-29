package consul

import (
	"fmt"
	"sync"

	"github.com/hashicorp/consul/api"
)

type ConsulProvider struct {
	sync.Mutex
	client *api.Client

	servicesWatches map[string]*consulWatch
}

func NewConsulProvider(addrPort, datacenter, token string) (*ConsulProvider, error) {
	c := api.DefaultConfig()
	if addrPort != "" {
		c.Address = addrPort
	}
	if datacenter != "" {
		c.Datacenter = datacenter
	}
	if token != "" {
		c.Token = token
	}
	client, err := api.NewClient(c)
	if err != nil {
		return nil, fmt.Errorf("can't initialize consul client: %s", err)
	}

	return &ConsulProvider{
		Mutex:           sync.Mutex{},
		client:          client,
		servicesWatches: map[string]*consulWatch{},
	}, nil
}

// func (cs *ConsulProvider) Shutdown() {
// 	cs.Lock()
// 	cs.Unlock()
// 	for name, w := range cs.servicesWatches {
// 		w.Shutdown()
// 	}
// }

func (cs *ConsulProvider) RegisterService(service *ConsulService) {
	cs.Lock()
	defer cs.Unlock()
	watcher, ok := cs.servicesWatches[service.Service]
	if !ok {
		// Not created yet !
		watcher = newConsulWatch(cs.client, service.Service)
		cs.servicesWatches[service.Service] = watcher
		watcher.Run()
	}

	watcher.Add(service)
}
