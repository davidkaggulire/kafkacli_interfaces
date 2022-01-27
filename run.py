# run.py

import sys
from broker.kafka_broker import KafkaBroker

if __name__ == '__main__':
    args = KafkaBroker.parsed_args(sys.argv[1:])

    if args['command'] == "send":
        KafkaBroker.sendMessages(args)
    elif args['command'] == "receive":
        KafkaBroker.readMessages(args)
