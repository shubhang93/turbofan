#!/usr/bin/env bash

set -e

brokers="${BOOTSTRAP_SERVERS:-"localhost:9092"}"
consumer_group="${CONSUMER_GROUP:-"test_consumer_0002"}"
consumer_count="${CONSUMER_COUNT:-1}"
topics="${TOPICS:-plain-text-log}"

go build -race -o out/relcon ./cmd
./out/parallelcons run -bootstrap-server "$brokers" \
  -consumer-group "$consumer_group" \
  -topics "$topics" \
  -consumer-count "$consumer_count"
