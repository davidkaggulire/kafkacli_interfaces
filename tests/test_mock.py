# tests.py

import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import unittest
from unittest.mock import patch
from confluent_kafka import Consumer
from broker.kafka_broker import KafkaBroker
from unittest import mock


class TestChatApp(unittest.TestCase):
    def setUp(self) -> None:
        self.broker = KafkaBroker()
        return self.broker

    def test_empty_parsed_args(self):
        """test parsed_args func with empty args"""
        with self.assertRaises(SystemExit):
            self.broker.parsed_args(sys.argv[1:])

    def test_normal_parser(self):
        """testing parsing args to function"""
        parser = self.broker.parsed_args(
            ['send', '--channel', 'chat', '--server', 'localhost:9092'])
        self.assertIsInstance(parser, dict)

    @patch('broker.kafka_broker.KafkaBroker')
    def test_parser_send(self, test_patch):
        """testing parsing command send"""
        test_patch.return_value = {
            'command': 'send',
            'channel': 'chat',
            'server': 'localhost:9092',
            'group': None,
            'from': 'start'
        }
        parser = self.broker.parsed_args(
            ['send', '--channel', 'chat', '--server', 'localhost:9092'])
        self.assertIsInstance(parser, dict)
        self.assertEqual(parser, test_patch.return_value)

    @patch('broker.kafka_broker.KafkaBroker')
    def test_parser_receive(self, test_patch):
        """testing parsing command receive"""
        test_patch.return_value = {
            'command': 'receive',
            'channel': 'chat',
            'server': 'localhost:9092',
            'group': None,
            'from': 'start'
        }
        parser = self.broker.parsed_args(
            ['receive', '--channel', 'chat', '--server',
                'localhost:9092', '--from', 'start'])
        self.assertIsInstance(test_patch.return_value, dict)
        self.assertEqual(parser, test_patch.return_value)

    def test_get_input_func(self):
        """test get_input function"""
        with patch('builtins.input', return_value='y'):
            assert KafkaBroker.get_input() == 'y'
        with patch('builtins.input', return_value='q'):
            self.assertRaises(SystemExit)
        with self.assertRaises(SystemExit) as cm:
            with patch('builtins.input', return_value='q'):
                KafkaBroker.get_input()
        self.assertEqual(cm.exception.code, 0)

    # @patch('broker.kafka_broker.KafkaBroker')
    def test_send_messages(self):
        """testing sending of message via kafka"""
        args = {
            'command': 'send',
            'channel': 'chat',
            'server': 'localhost:9092',
            'group': "hello",
            'from': 'start',
            "message": "stopsession"
        }
        sent = self.broker.sendMessages(args)
        self.assertEqual(sent, True)

    # @patch('broker.kafka_broker.KafkaBroker')
    def test_read_messages(self):
        """test reading message from Kafka"""
        data = {
            'command': 'send',
            'channel': 'chat',
            'server': 'localhost:9092',
            'group': "hello",
            'from': 'start',
            "message": "stopsession"
        }
        args = {
            'command': 'send',
            'channel': 'chat',
            'server': 'localhost:9092',
            'group': "hello",
            'from': 'start'
        }
        self.broker.sendMessages(data)
        received = self.broker.readMessages(args)
        self.assertEqual(True, received)

    @patch('broker.kafka_broker.KafkaBroker')
    def test_consume_messages(self, mock_consumer):
        args = {'command': 'receive', 'channel': 'chat',
                'server': 'localhost:9092', 'group': 'hello', 'from': 'start'}
        arg2 = {
            'bootstrap.servers': args['server'],
            'group.id': args['group'],
            'auto.offset.reset': 'beginning'
        }
        mock_consumer.return_value = Consumer(arg2)

        result = KafkaBroker.consume_messages(args)

        self.assertIsInstance(mock_consumer.return_value, Consumer)
        self.assertIsInstance(result, Consumer)

    def test_encode_func(self):
        args = {
            'command': 'send',
            'channel': 'chat',
            'server': 'localhost:9092',
            'group': "hello",
            'from': 'start',
            "message": "stopsession"
        }
        result = KafkaBroker.encode_func(args)
        self.assertEqual(b'stopsession', result)

    @patch('broker.kafka_broker.KafkaBroker')
    def test_delivery_report_error(self, mock_delivery):
        mock_delivery.return_value = mock.Mock("Message delivery failed: err")
        result = KafkaBroker.delivery_report("err", "msg")
        self.assertEqual("Message delivery failed: err", result)
        self.assertFalse(mock_delivery.called)


if __name__ == '__main__':
    unittest.main()
