# turbofan

Parallelize Kafka processing by utilising the power of native go routines

### What does this library enable you to do?

- We know that the max number of threads which can be used to process kafka messages is equal to the number of
  partitions in a topic. Any number of threads beyond the number of partitions would remain in an idle state.
- This library lets you bypass the above restriction by enabling you to process messages in as many go routines as you
  want which can be greater than the number of partitions for a topic.
- Turbofan lets you commit (ACK) messages in any order you want, enabling out-of-order processing without the user
  having to manage the offset state.

### How to use

```shell
go get github.com/shubhang93/turbofan
```

Refer to the `examples` directory for both ordered and parallel processing

### Turbofan Configuration

`* - required configuration`

```
{
	BootstrapServers*  []string  // Kafka bootstrap servers []string{"localhost:9092"}
	MessageBatchSize   int       // Message batch size to be delivered, default 500
	SessionTimeoutMS   int       // Consumer session timeout default 10000
	MaxPollIntervalMS  int       // Maximum Time interval between polls before a consumer can be pronounced dead, default 15000 
	LogLevels          []string  // Consumer log levels default []string{"consumer"}
	ConsumerGroupID*   string    // Consumer Group ID
	AutoOffsetReset    string    // Strategy for handling offset resets, default "latest"
	CommitIntervalMS   int       // Frequency of commit calls to Kafka broker, default 5000
}
```

