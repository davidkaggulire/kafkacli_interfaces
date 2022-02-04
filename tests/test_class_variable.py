# test_listener.py

import sys
import os
import pytest
import threading
import logging
import concurrent.futures
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from broker.kafka_broker_listener import KafkaBrokerListener


@pytest.fixture
def service(getBroker):
    message_app = KafkaBrokerListener()
    print(message_app)
    return message_app


@pytest.fixture
def getBroker():
    """return broker"""
    return KafkaBrokerListener()


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

    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    
    event = threading.Event()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(service.readMessages, data2)
        time.sleep(1)
        
        # set listener to stop  consumer from running
        service.set_listener()

    output = service.readMessages(data2)
    assert output is True
