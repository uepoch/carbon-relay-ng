package consul

import (
	"sync"
	"time"

	"github.com/jpillora/backoff"

	"github.com/hashicorp/consul/api"

	"github.com/sirupsen/logrus"
)

type consulWatch struct {
	sync.RWMutex
	client      *api.Client
	Service     string
	subscribers []*ConsulService
	lastState   []*api.ServiceEntry

	// shutDownCh chan struct{}
}

func newConsulWatch(client *api.Client, service string) *consulWatch {
	return &consulWatch{
		RWMutex:     sync.RWMutex{},
		client:      client,
		Service:     service,
		subscribers: []*ConsulService{},
		lastState:   []*api.ServiceEntry{},
		// shutDownCh:  make(chan struct{}),
	}
}

// No logic yet
// func (w *consulWatch) Shutdown() {
// 	close(w.shutDownCh)
// }

func (w *consulWatch) watch() {
	backoff := backoff.Backoff{
		Min: 500 * time.Millisecond,
		Max: 20 * time.Second,
	}
	var index uint64

	for {
		services, meta, err := w.client.Health().Service(w.Service, "", false, &api.QueryOptions{
			AllowStale: true,
			WaitTime:   10 * time.Minute,
			WaitIndex:  index,
		})
		if err != nil {
			logrus.Errorf("can't fetch health information for service %s: %s", w.Service, err)
			time.Sleep(backoff.Duration())
			continue
		}
		index = meta.LastIndex
		w.Lock()
		w.lastState = services
		w.Unlock()
		w.RLock()
		defer w.RUnlock()
		for _, sub := range w.subscribers {
			sub.process(services)
		}
	}
}

func (w *consulWatch) Run() {
	go w.watch()
}

func (w *consulWatch) Add(service *ConsulService) {
	w.Lock()
	defer w.Unlock()
	w.subscribers = append(w.subscribers)
	service.process(w.lastState)
}
