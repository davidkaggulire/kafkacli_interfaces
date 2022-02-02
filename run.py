# run.py

import sys
from broker.kafka_broker import KafkaBroker


if __name__ == '__main__':
    broker_kafka = KafkaBroker()
    args = broker_kafka.parsed_args(sys.argv[1:])
    print(args)

    if args['command'] == "send":
        broker_kafka.sendMessages(args)
    elif args['command'] == "receive":
        broker_kafka.readMessages(args)
