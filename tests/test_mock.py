# tests.py

from re import T
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import unittest
from unittest.mock import patch
from confluent_kafka import Consumer, Producer
from broker.kafka_mock import KafkaBrokerMock
from unittest import mock


class TestChatApp(unittest.TestCase):
    def setUp(self) -> None:
        self.broker = KafkaBrokerMock()
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

    @patch('broker.kafka_mock.KafkaBrokerMock')
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

    @patch('broker.kafka_mock.KafkaBrokerMock')
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
            assert KafkaBrokerMock.get_input() == 'y'
        with patch('builtins.input', return_value='q'):
            self.assertRaises(SystemExit)
        with self.assertRaises(SystemExit) as cm:
            with patch('builtins.input', return_value='q'):
                KafkaBrokerMock.get_input()
        self.assertEqual(cm.exception.code, 0)

    # # @patch('broker.kafka_broker.KafkaBroker')
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

    @patch('broker.kafka_mock.Producer')
    def test_produce_method(self, mock_producer):
        """testing sending of message via kafka"""
        args = {
            'command': 'send',
            'channel': 'chat',
            'server': 'localhost:9092',
            'group': "hello",
            'from': 'start',
            "message": "Welcome to Uganda"
        }
        mock_producer.return_value = mock.Mock()
        mock_producer.produce = None
        response = KafkaBrokerMock.produce_method(args)
        self.assertTrue(response, mock_producer)

    @patch('broker.kafka_mock.Producer')
    def test_flush_method(self, mock_producer):
        """testing sending of message via kafka"""
        args = {
            'command': 'send',
            'channel': 'chat',
            'server': 'localhost:9092',
            'group': "hello",
            'from': 'start',
            "message": "Welcome to Uganda"
        }
        # mock_producer.return_value = mock.Mock()
        mock_producer.return_value = mock.Mock()
        mock_producer.flush = False
        response = KafkaBrokerMock.flush_method(args)
        self.assertTrue(response, mock_producer)

    @patch('broker.kafka_mock.Producer')
    def test_send_messages(self, mock_producer):
        """testing sending of message via kafka"""
        args = {
            'command': 'send',
            'channel': 'chat',
            'server': 'localhost:9092',
            'group': "hello",
            'from': 'start',
            "message": "Welcome to Uganda"
        }
        mock_producer.return_value = mock.Mock()
        mock_producer.flush = False
        # response = KafkaBrokerMock.flush_method(args)
        response = self.broker.sendMessages(args)
        self.assertEqual(response, True)


    # @patch('broker.kafka_broker.KafkaBroker')
    # def test_read_messages(self):
    #     """test reading message from Kafka"""
    #     data = {
    #         'command': 'send',
    #         'channel': 'chat',
    #         'server': 'localhost:9092',
    #         'group': "hello",
    #         'from': 'start',
    #         "message": "stopsession"
    #     }
    #     args = {
    #         'command': 'send',
    #         'channel': 'chat',
    #         'server': 'localhost:9092',
    #         'group': "hello",
    #         'from': 'start'
    #     }
    #     self.broker.sendMessages(data)
    #     received = self.broker.readMessages(args)
    #     self.assertEqual(True, received)

    @patch('broker.kafka_mock.KafkaBrokerMock')
    def test_consume_messages(self, mock_consumer):
        args = {'command': 'receive', 'channel': 'chat',
                'server': 'localhost:9092', 'group': 'hello', 'from': 'start'}
        arg2 = {
            'bootstrap.servers': args['server'],
            'group.id': args['group'],
            'auto.offset.reset': 'beginning'
        }
        mock_consumer.return_value = Consumer(arg2)

        result = KafkaBrokerMock.consume_messages(args)

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
        result = KafkaBrokerMock.encode_func(args)
        self.assertEqual(b'stopsession', result)

    @patch('broker.kafka_mock.KafkaBrokerMock')
    def test_delivery_report_error(self, mock_delivery):
        mock_delivery.return_value = mock.Mock("Message delivery failed: err")
        result = KafkaBrokerMock.delivery_report("err", "msg")
        self.assertEqual("Message delivery failed: err", result)
        self.assertFalse(mock_delivery.called)


if __name__ == '__main__':
    unittest.main()
