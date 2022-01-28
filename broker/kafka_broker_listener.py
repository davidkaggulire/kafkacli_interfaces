# kafka_broker_listener.py

from confluent_kafka import Producer, Consumer
import argparse
import sys
from .broker_interface import IMessageBroker


class KafkaBrokerListener(IMessageBroker):

    def parsed_args(self, args):
        """
        parse command line arguments needed for Kafka
        """
        parser = argparse.ArgumentParser(description="Send and receive messages using Kafka CLI")
        parser.add_argument('command', choices=['send', 'receive'], help="Parse in either send or receive")
        parser.add_argument('--channel', required=True, help="the channel or topic to send the message to")
        parser.add_argument('--server', required=True, help="server connection")
        parser.add_argument('--group', help="group to send messages to")
        parser.add_argument('--from', choices=['start', 'latest'], default='start', help="choose which messages by start or latest")
        args = parser.parse_args(args)
        print(vars(args))
        return vars(args)

    def get_input():
        data = input("Enter message to send or 'q' to quit: ")
        if data == 'q':
            sys.exit(0)
        return data

    def encode_func(args):
        returned_data = args["message"]
        return returned_data.encode('utf-8')

    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err:
            return 'Message delivery failed: {}'.format(err)
        else:
            return 'Message delivered to {} [{}]'.format(msg.topic(), msg.partition())

    def sendMessages(self, args):
        """
        function to send messages
        """
        connection = args['server']
        channel = args['channel']
        group = args['group']
        from_where = args['from']

        print(f"You have decided to send to the channel: {channel}")
        print(f"You are sending through the server: {connection}")
        print(f"You are sending through the group: {group}")
        print(f"You are sending through the group: {from_where}")

        p = Producer({'bootstrap.servers': connection})

        data = KafkaBrokerListener.encode_func(args)

        p.produce(channel, data, callback=KafkaBrokerListener.delivery_report)
        p.flush()
        return True

    def consume_messages(args):
        """
        function to read messages
        """
        channel = args['channel']
        server = args['server']
        start_from = args['from']
        group = args['group']

        print(f"You are receiving from the channel: {channel}")
        print(f"You are receiving from the {start_from}")
        print(f"You are receiving through the server {server}")
        print(f"You are part of the receiving group: {group}")

        message_level = {
            'start': 'beginning',
            'latest': 'latest'
        }
        c = Consumer({
            'bootstrap.servers': args['server'],
            'group.id': args['group'],
            'auto.offset.reset': message_level[args['from']],
            # 'default.api.timeout.ms': 1000
            "enable.auto.commit": True
        })

        return c

    def readMessages(self, args):
        c = KafkaBrokerListener.consume_messages(args)
        c.subscribe([args['channel']])

        try:
            while True:
                msg = c.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue

                var = msg.value().decode('utf-8')
                print(f"Received message: {var}")

                # unsubscribing or unassigning topic from consumer
                if args['channel'] == "stoplistener":
                    c.unassign([args['channel']])
                    # c.unsubscribe([args['channel']])

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')
        finally:
            c.close()
            return True
