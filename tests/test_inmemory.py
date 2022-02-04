# test_listener.py

import sys
import os
import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from broker.kafka_broker_listener import KafkaBrokerListener
from broker.inmemory_broker import InMemoryBroker
from message_system import MessagingSystem


@pytest.fixture
def service(getBroker):
    message_app = MessagingSystem(getBroker)
    print(message_app)
    return message_app


@pytest.fixture
def getBroker():
    """return broker"""
    # return KafkaBrokerListener()
    return InMemoryBroker()


def test_send_messages(service):
    print(service)
    data = {
        'command': 'send',
        'channel': 'stoplistener',
        'server': 'localhost:9092',
        'group': "hello",
        'from': 'start',
        "message": "welcome Uganda"
    }
    output = service.sendMessages(data)
    assert output is True


def test_read_messages(service):
    data = {
        'command': 'send',
        'channel': 'stoplistener',
        'server': 'localhost:9092',
        'group': "hello",
        'from': 'start',
        "message": "welcome Uganda"
    }
    service.sendMessages(data)

    data2 = {
        'command': 'send',
        'channel': 'stoplistener',
        'server': 'localhost:9092',
        'group': "hello",
        'from': 'start',

    }
    output = service.readMessages(data2)
    assert output is True
