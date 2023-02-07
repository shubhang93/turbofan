# Kafka concurrent consumer POC in Golang

## This is a POC for a non-blocking Kafka consumer polling, which keeps the consumer alive even if the processing takes longer `max.poll.interval.ms`

## How to run 

## Prerequisites
- Requires a Kafka Broker / cluster to be running locally if connecting to a local broker

```shell
# export BOOTSTRAP_SERVERS=localhost:9092
# export CONSUMER_GROUP=test_consumer_0001
# export CONSUMER_COUNT=1
# export TOPICS=plain-text-log
./script/run.sh
```

## Metrics
- Spin up the prometheus container on your machine by running `docker-compose up -d`
- Open `localhost:3000` and add `prometheus:9090` as the datasource
- Explore the metrics under the prefix `wip_con_consumer_golang`

# relcon
