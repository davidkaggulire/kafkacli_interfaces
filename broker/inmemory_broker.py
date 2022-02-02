# inmemory_broker.py

import concurrent.futures
import logging
import queue
import threading
import time

from .broker_interface import IMessageBroker


class InMemoryBroker(IMessageBroker):

    def __init__(self):
        self.event = threading.Event()
        self.pipeline = queue.Queue(maxsize=2)

    def sendMessages(self, args: dict) -> bool:
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                            datefmt="%H:%M:%S")
        logging.getLogger().setLevel(logging.DEBUG)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:  
            executor.submit(InMemoryBroker.producer, self.pipeline, self.event, args)

            time.sleep(0.1)
            # logging.info("Main: about to set event")
            self.event.set()
        return True

    def readMessages(self, args: dict) -> bool:
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                            datefmt="%H:%M:%S")
        logging.getLogger().setLevel(logging.DEBUG)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(InMemoryBroker.consumer, self.pipeline, self.event)

            time.sleep(0.1)
            logging.info("Main: about to set event")
            self.event.set()
        return True

  
    def producer(queue, event, args):
        """Pretend we're getting a number from the network."""
        while not event.is_set():
            message = args['message']
            queue.put(message)
            logging.info(f"Producer got message: {message}")
            
            # if message == "stop session":
            #     break
            break
        
        logging.info("Producer received event. Exiting")
        

    def consumer(queue, event):
        """Pretend we're saving a number in the database."""
        while not event.is_set() or not queue.empty():
            message = queue.get()
            logging.info(
                f"Consumer received and storing message: {message}"
            )

        logging.info("Consumer received event. Exiting")
