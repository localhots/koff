# Koff

Kafka consumer (group) offset tracker.

### WIP

Work in progress, there's plenty of stuff left to figure out. You have been 
warned.

### Usage

So far it is only cable of printing parsed messages. For usage eample take a 
look at the main command.

```
go run cmd/printer/main.go -brokers 127.0.0.1:9092
```

### Design

Starting with Kafka version 0.9 consumer offsets are stored and managed by the
Kafka server. Internally offsets are stored in the `__consumer_offsets` topic.
It is not designed to be used by third party software but nothing stops us from
doing that really. 

This topic is not only used to store individual consumer offsets, it also 
contains consumer group metadata: list of group members and their subscriptions
and assignments, leader details and plenty of other things. Given that the topic
provides realtime updates on consumer offsets and consumer group structure and
state, it makes it a very convenient foundation for consumer or group tracking
and monitoring.

### Licence

MIT