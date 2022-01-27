# tests.py

import sys
import os
import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from broker.kafka_broker import KafkaBroker
from message_system import MessagingSystem


@pytest.fixture
def service(getBroker):
    message_app = MessagingSystem(getBroker)
    print(message_app)
    return message_app


@pytest.fixture
def getBroker():
    """return broker"""
    return KafkaBroker()


def test_send_messages(service):
    print(service)
    data = {
        'command': 'send',
        'channel': 'chat',
        'server': 'localhost:9092',
        'group': "hello",
        'from': 'start',
        "message": "welcome Uganda"
    }
    output = service.sendMessages(data)
    assert output is True


def test_read_messages(service):
    data2 = {
        'command': 'send',
        'channel': 'chat',
        'server': 'localhost:9092',
        'group': "hello",
        'from': 'start',

    }
    output = service.readMessages(data2)
    assert output is True

    # cleaning up the test
    data = {
        'command': 'send',
        'channel': 'chat',
        'server': 'localhost:9092',
        'group': "hello",
        'from': 'start',
        "message": "stopsession"
    }
    service.sendMessages(data)
