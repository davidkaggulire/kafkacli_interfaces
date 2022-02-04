# message_system

from broker import IMessageBroker, KafkaBroker, KafkaBrokerListener, InMemoryBroker
from broker import KafkaBrokerMock


class MessagingSystem:

    def __init__(self, message_broker: IMessageBroker) -> None:
        self.mb = message_broker

    def readMessages(self, data: dict) -> bool:
        received = self.mb.readMessages(data)
        if received:
            return True
        else:
            return False

    def sendMessages(self, data: dict) -> bool:
        sent = self.mb.sendMessages(data)
        if sent:
            return True
        else:
            return False


data = {
    'command': 'send',
    'channel': 'talkchat',
    'server': 'localhost:9092',
    'group': "hello",
    'from': 'start',
    "message": "Welcome to Uganda"
}

# data = {
#     'command': 'send',
#     'channel': 'talkchat',
#     'server': 'localhost:9092',
#     'group': "hello",
#     'from': 'start',
#     "message": "stop session"
# }

data2 = {
    'command': 'receive',
    'channel': 'talkchat',
    'server': 'localhost:9092',
    'group': "hello",
    'from': 'start',
}

# ms = MessagingSystem(KafkaBroker())
# ms.sendMessages(data)
# ms.readMessages(data2)

# ms = MessagingSystem(KafkaBrokerListener())
# ms.sendMessages(data)
# ms.readMessages(data2)

# ms = MessagingSystem(InMemoryBroker())
# ms.sendMessages(data)
# ms.readMessages(data2)
