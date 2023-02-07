package consumer

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Config struct {
	BootstrapServers  []string
	MessageBatchSize  int
	SessionTimeoutMS  int
	MaxPollIntervalMS int
	LogLevels         []string
	ConsumerGroupID   string
	AutoOffsetReset   string
	CommitIntervalMS  int
}

func (c Config) toConfigMap() kafka.ConfigMap {
	cm := kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(c.BootstrapServers, ","),
		"debug":                    "consumer",
		"group.id":                 c.ConsumerGroupID,
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
		"session.timeout.ms":       10000,
		"auto.offset.reset":        "latest",
		"max.poll.interval.ms":     15000,
		"enable.partition.eof":     true,
	}

	if c.AutoOffsetReset != "" {
		cm["auto.offset.reset"] = c.AutoOffsetReset
	}

	if c.SessionTimeoutMS > 0 {
		cm["session.timeout.ms"] = c.SessionTimeoutMS
	}

	if c.MaxPollIntervalMS > 0 {
		cm["max.poll.interval.ms"] = c.MaxPollIntervalMS
	}

	if c.LogLevels != nil {
		cm["debug"] = strings.Join(c.LogLevels, ",")
	}

	return cm

}
