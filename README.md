# kafkacli_interfaces
A project to experiment with message broker configurations while making use of software design patterns i.e. interfaces

## How it works
To send a message run the app and parse arguments i.e.

```python run.py send --channel channelname --server "server:port" --group group_name```

- send - The command
- channel - The channel to send the message to.
- group - A group to send messages to. Group is optional

To receive a message via the app run

```python run.py receive --channel channelname --from start_from --server "server:port" --group group_name```

- receive - The command
- channel - The channel to receive the message from
- from - The point to start receiving messages from, options will be either ```start | latest```. If set to “start”, then this receiver will receive all messages it has not yet received before. The “latest” option means only messages sent while the receiver is up will be received
- group - A group to receive messages from. Group is optional

- The option, --server “server:port”, is where the Kafka connection string will be supplied

### Tests
To run the tests, use 
```pytest --cov=tests/```

### Generate coverage report
Use the commands below to print out a simple command-line report.

coverage report -m


## Important to note
Apache Kafka and zookeeper should be installed on your machine. 
Create a topic of which the producer shall send messages to and consumer receive messages from. This can be done using the command below

```$ bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1```

More information can be got from http://kafka.apache.org/documentation/#quickstart