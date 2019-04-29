package cfg

type Discovery struct {
	Consul *Consul
}

type Consul struct {
	Address    string
	Token      string
	Datacenter string
}
