# inmemory_broker.py

import logging

from .broker_interface import IMessageBroker


class InMemoryBroker(IMessageBroker):

    def __init__(self):
        self.pipeline = []

    def sendMessages(self, args: dict) -> bool:
        self.producer(self.pipeline, args)
        return True

    def readMessages(self, args: dict) -> bool:
        self.consumer(self.pipeline)
        return True

    def producer(self, queue, args):
        """getting message from the network."""  
        message = args['message']
        # saving message to broker
        queue.append(message)
        print(queue)
        print(f"Producer got message: *{message}* from network")
        print("Producer... Exiting")
        
    def consumer(self, queue):
        """retrieving received message."""
        message = queue.pop(0)
        print(
            f"Consumer received and storing message: {message}"
        )
        print("Consumer... Exiting")
