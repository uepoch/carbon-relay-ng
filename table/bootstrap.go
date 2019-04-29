package table

import (
	"fmt"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/graphite-ng/carbon-relay-ng/aggregator"
	"github.com/graphite-ng/carbon-relay-ng/badmetrics"
	"github.com/graphite-ng/carbon-relay-ng/cfg"
	"github.com/graphite-ng/carbon-relay-ng/imperatives"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/rewriter"
	"github.com/graphite-ng/carbon-relay-ng/route"
	log "github.com/sirupsen/logrus"
)

func InitFromConfig(config cfg.Config, meta toml.MetaData) (*Table, error) {
	table := New(config)

	err := table.InitBadMetrics(config)
	if err != nil {
		return table, err
	}

	err = table.InitCmd(config)
	if err != nil {
		return table, err
	}

	err = table.InitDiscovery(config)
	if err != nil {
		return table, err
	}

	err = table.InitBlacklist(config)
	if err != nil {
		return table, err
	}

	err = table.InitAggregation(config)
	if err != nil {
		return table, err
	}

	err = table.InitRewrite(config)
	if err != nil {
		return table, err
	}

	err = table.InitRoutes(config, meta)
	if err != nil {
		return table, err
	}

	return table, nil
}

func (table *Table) InitBadMetrics(config cfg.Config) error {
	maxAge, err := time.ParseDuration(config.Bad_metrics_max_age)
	if err != nil {
		log.Errorf("could not parse badMetrics max age: %s", err.Error())
		return fmt.Errorf("could not initialize bad metrics")
	}
	table.bad = badmetrics.New(maxAge)

	return nil
}

func (table *Table) InitCmd(config cfg.Config) error {
	for i, cmd := range config.Init.Cmds {
		log.Infof("applying: %s", cmd)
		err := imperatives.Apply(table, cmd)
		if err != nil {
			log.Errorf("could not apply init cmd #%d: %s", i+1, err.Error())
			return fmt.Errorf("could not apply init cmd #%d", i+1)
		}
	}

	return nil
}

func (table *Table) InitBlacklist(config cfg.Config) error {
	for i, entry := range config.BlackList {
		parts := strings.SplitN(entry, " ", 2)
		if len(parts) < 2 {
			return fmt.Errorf("invalid blacklist cmd #%d", i+1)
		}

		prefix := ""
		sub := ""
		regex := ""

		switch parts[0] {
		case "prefix":
			prefix = parts[1]
		case "sub":
			sub = parts[1]
		case "regex":
			regex = parts[1]
		default:
			return fmt.Errorf("invalid blacklist method for cmd #%d: %s", i+1, parts[1])
		}

		m, err := matcher.New(prefix, sub, regex)
		if err != nil {
			log.Error(err.Error())
			return fmt.Errorf("could not apply blacklist cmd #%d", i+1)
		}

		table.AddBlacklist(m)
	}

	return nil
}

func (table *Table) InitAggregation(config cfg.Config) error {
	for i, aggConfig := range config.Aggregation {
		agg, err := aggregator.New(aggConfig.Function, aggConfig.Regex, aggConfig.Prefix, aggConfig.Substr, aggConfig.Format, aggConfig.Cache, uint(aggConfig.Interval), uint(aggConfig.Wait), aggConfig.DropRaw, table.In)
		if err != nil {
			log.Error(err.Error())
			return fmt.Errorf("could not add aggregation #%d", i+1)
		}

		table.AddAggregator(agg)
	}

	return nil
}

func (table *Table) InitRewrite(config cfg.Config) error {
	for i, rewriterConfig := range config.Rewriter {
		rw, err := rewriter.New(rewriterConfig.Old, rewriterConfig.New, rewriterConfig.Not, rewriterConfig.Max)
		if err != nil {
			log.Error(err.Error())
			return fmt.Errorf("could not add rewriter #%d", i+1)
		}

		table.AddRewriter(rw)
	}

	return nil
}

func (table *Table) InitRoutes(config cfg.Config, meta toml.MetaData) error {
	for _, routeConfig := range config.Route {
		switch routeConfig.Type {
		case "sendAllMatch":
			destinations, err := imperatives.ParseDestinations(routeConfig.Destinations, table, true, routeConfig.Key)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("could not parse destinations for route '%s'", routeConfig.Key)
			}
			if len(destinations) == 0 {
				return fmt.Errorf("must get at least 1 destination for route '%s'", routeConfig.Key)
			}

			route, err := route.NewSendAllMatch(routeConfig.Key, routeConfig.Prefix, routeConfig.Substr, routeConfig.Regex, destinations)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("error adding route '%s'", routeConfig.Key)
			}
			table.AddRoute(route)
		case "sendFirstMatch":
			destinations, err := imperatives.ParseDestinations(routeConfig.Destinations, table, true, routeConfig.Key)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("could not parse destinations for route '%s'", routeConfig.Key)
			}
			if len(destinations) == 0 {
				return fmt.Errorf("must get at least 1 destination for route '%s'", routeConfig.Key)
			}

			route, err := route.NewSendFirstMatch(routeConfig.Key, routeConfig.Prefix, routeConfig.Substr, routeConfig.Regex, destinations)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("error adding route '%s'", routeConfig.Key)
			}
			table.AddRoute(route)
		case "consistentHashing":
			destinations, err := imperatives.ParseDestinations(routeConfig.Destinations, table, false, routeConfig.Key)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("could not parse destinations for route '%s'", routeConfig.Key)
			}
			if len(destinations) < 2 {
				return fmt.Errorf("must get at least 2 destination for route '%s'", routeConfig.Key)
			}

			route, err := route.NewConsistentHashing(routeConfig.Key, routeConfig.Prefix, routeConfig.Substr, routeConfig.Regex, destinations)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("error adding route '%s'", routeConfig.Key)
			}
			table.AddRoute(route)
		case "grafanaNet":
			var spool bool
			sslVerify := true
			var bufSize = int(1e7) // since a message is typically around 100B this is 1GB
			var flushMaxNum = 5000 // number of metrics
			var flushMaxWait = 500 // in ms
			var timeout = 10000    // in ms
			var concurrency = 100  // number of concurrent connections to tsdb-gw
			var orgId = 1

			// by merely looking at routeConfig.SslVerify we can't differentiate between the user not specifying the property
			// (which should default to false) vs specifying it as false explicitly (which should set it to false), as in both
			// cases routeConfig.SslVerify would simply be set to false.
			// So, we must look at the metadata returned by the config parser.

			routeMeta := meta.Mapping["route"].([]map[string]interface{})

			// Note: toml library allows arbitrary casing of properties,
			// and the map keys are these properties as specified by user
			// so we can't look up directly
		OuterLoop:
			for _, routemeta := range routeMeta {
				for k, v := range routemeta {
					if strings.ToLower(k) == "key" && v == routeConfig.Key {
						for k2, v2 := range routemeta {
							if strings.ToLower(k2) == "sslverify" {
								sslVerify = v2.(bool)
								break OuterLoop
							}
						}
					}
				}
			}

			if routeConfig.Spool {
				spool = routeConfig.Spool
			}
			if routeConfig.BufSize != 0 {
				bufSize = routeConfig.BufSize
			}
			if routeConfig.FlushMaxNum != 0 {
				flushMaxNum = routeConfig.FlushMaxNum
			}
			if routeConfig.FlushMaxWait != 0 {
				flushMaxWait = routeConfig.FlushMaxWait
			}
			if routeConfig.Timeout != 0 {
				timeout = routeConfig.Timeout
			}
			if routeConfig.Concurrency != 0 {
				concurrency = routeConfig.Concurrency
			}
			if routeConfig.OrgId != 0 {
				orgId = routeConfig.OrgId
			}

			route, err := route.NewGrafanaNet(routeConfig.Key, routeConfig.Prefix, routeConfig.Substr, routeConfig.Regex, routeConfig.Addr, routeConfig.ApiKey, routeConfig.SchemasFile, spool, sslVerify, routeConfig.Blocking, bufSize, flushMaxNum, flushMaxWait, timeout, concurrency, orgId)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("error adding route '%s'", routeConfig.Key)
			}
			table.AddRoute(route)
		case "kafkaMdm":
			var bufSize = int(1e7)  // since a message is typically around 100B this is 1GB
			var flushMaxNum = 10000 // number of metrics
			var flushMaxWait = 500  // in ms
			var timeout = 2000      // in ms
			var orgId = 1

			if routeConfig.PartitionBy != "byOrg" && routeConfig.PartitionBy != "bySeries" {
				return fmt.Errorf("invalid partitionBy for route '%s'", routeConfig.Key)
			}

			if routeConfig.BufSize != 0 {
				bufSize = routeConfig.BufSize
			}
			if routeConfig.FlushMaxNum != 0 {
				flushMaxNum = routeConfig.FlushMaxNum
			}
			if routeConfig.FlushMaxWait != 0 {
				flushMaxWait = routeConfig.FlushMaxWait
			}
			if routeConfig.Timeout != 0 {
				timeout = routeConfig.Timeout
			}
			if routeConfig.OrgId != 0 {
				orgId = routeConfig.OrgId
			}

			route, err := route.NewKafkaMdm(routeConfig.Key, routeConfig.Prefix, routeConfig.Substr, routeConfig.Regex, routeConfig.Topic, routeConfig.Codec, routeConfig.SchemasFile, routeConfig.PartitionBy, routeConfig.Brokers, bufSize, orgId, flushMaxNum, flushMaxWait, timeout, routeConfig.Blocking)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("error adding route '%s'", routeConfig.Key)
			}
			table.AddRoute(route)
		case "pubsub":
			var codec = "gzip"
			var format = "plain"                    // aka graphite 'linemode'
			var bufSize = int(1e7)                  // since a message is typically around 100B this is 1GB
			var flushMaxSize = int(1e7) - int(4096) // 5e6 = 5MB. max size of message. Note google limits to 10M, but we want to limit to less to account for overhead
			var flushMaxWait = 1000                 // in ms

			if routeConfig.Codec != "" {
				codec = routeConfig.Codec
			}
			if routeConfig.Format != "" {
				format = routeConfig.Format
			}
			if routeConfig.BufSize != 0 {
				bufSize = routeConfig.BufSize
			}
			if routeConfig.FlushMaxSize != 0 {
				flushMaxSize = routeConfig.FlushMaxSize
			}
			if routeConfig.FlushMaxWait != 0 {
				flushMaxWait = routeConfig.FlushMaxWait
			}

			route, err := route.NewPubSub(routeConfig.Key, routeConfig.Prefix, routeConfig.Substr, routeConfig.Regex, routeConfig.Project, routeConfig.Topic, format, codec, bufSize, flushMaxSize, flushMaxWait, routeConfig.Blocking)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("error adding route '%s'", routeConfig.Key)
			}
			table.AddRoute(route)
		case "cloudWatch":
			var bufSize = int(1e7)            // since a message is typically around 100B this is 1GB
			var flushMaxSize = int(20)        // Amazon limits to 20 MetricDatum/PutMetricData request https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
			var flushMaxWait = 10000          // in ms
			var storageResolution = int64(60) // Default CloudWatch resolution is 60s
			var awsProfile = ""
			var awsRegion = ""
			var awsNamespace = ""
			var awsDimensions [][]string

			if routeConfig.BufSize != 0 {
				bufSize = routeConfig.BufSize
			}
			if routeConfig.FlushMaxSize != 0 {
				flushMaxSize = routeConfig.FlushMaxSize
			}
			if routeConfig.FlushMaxWait != 0 {
				flushMaxWait = routeConfig.FlushMaxWait
			}
			if routeConfig.Profile != "" {
				awsProfile = routeConfig.Profile
			}
			if routeConfig.Region != "" {
				awsRegion = routeConfig.Region
			}
			if routeConfig.Namespace != "" {
				awsNamespace = routeConfig.Namespace
			}
			if len(routeConfig.Dimensions) > 0 {
				awsDimensions = routeConfig.Dimensions
			}
			if routeConfig.StorageResolution != 0 {
				storageResolution = routeConfig.StorageResolution
			}

			route, err := route.NewCloudWatch(routeConfig.Key, routeConfig.Prefix, routeConfig.Substr, routeConfig.Regex, awsProfile, awsRegion, awsNamespace, awsDimensions, bufSize, flushMaxSize, flushMaxWait, storageResolution, routeConfig.Blocking)
			if err != nil {
				log.Error(err.Error())
				return fmt.Errorf("error adding route '%s'", routeConfig.Key)
			}
			table.AddRoute(route)
		default:
			return fmt.Errorf("unrecognized route type '%s'", routeConfig.Type)
		}
	}

	return nil
}

func (t *Table) InitDiscovery(config cfg.Config) error {

	return nil

}
