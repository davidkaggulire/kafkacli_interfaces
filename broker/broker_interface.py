# broker interface

from abc import ABC, abstractmethod


class IMessageBroker(ABC):

    @abstractmethod
    def sendMessages(self, args: dict) -> bool:
        """produce messages"""

    @abstractmethod
    def readMessages(self, args: dict) -> bool:
        """consume file"""
